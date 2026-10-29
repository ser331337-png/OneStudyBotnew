"""OneStudy v3: one process, persistent inbox/outbox and multiple orders."""
from __future__ import annotations
import asyncio
from collections import defaultdict
from datetime import datetime
from html import escape
import json
import logging
import os
from pathlib import Path
import time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramNetworkError
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, FSInputFile, BotCommand, ErrorEvent
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from db import Store, TERMINAL
from facts import FACTS

STATUSES = {'new':'🆕 Новая заявка','clarify':'🟡 Уточняем задание','payment':'💳 Ожидаем оплату',
 'working':'🛠 В работе','checking':'🔎 На проверке у клиента','revision':'✍️ На доработке',
 'done':'✅ Завершён','cancelled':'❌ Отменён','archive':'📁 Архив'}
WORK = ['Курсовая','Диплом / ВКР','Реферат','Презентация','Оригинальность текста','Другое']
SUBJECT = ['Экономика','Юриспруденция','Менеджмент','Психология','Педагогика','Информатика / IT','Технические науки','Медицина','Другое']
FIELDS = ['work','subject','deadline','anti','title']
QUESTIONS = ['Какая помощь нужна?','Укажите направление или дисциплину.',
 'Укажите дату сдачи: ДД.ММ.ГГГГ. При необходимости добавьте время ЧЧ:ММ.',
 'Будет ли проверка на антиплагиат? Укажите систему и процент, если знаете.',
 'Напишите тему задания. Документы и фотографии можно отправлять сюда в любой момент.']
WELCOME = 'Здравствуйте! Мы OneStudy, всегда готовы проконсультировать по учебным вопросам!\nВыберите, какая помощь нужна, или сразу напишите вопрос и прикрепите документы (при наличии).'
FAQ = {'pay':('💳 Оплата','50% предоплата. По мере готовности показываем фрагменты работы. После согласования и оплаты остатка передаём итоговые файлы.'),
 'fix':('🛡 Доработки','Правки в рамках исходного задания — бесплатно. Новые требования и дополнительный объём обсуждаются отдельно.'),
 'anti':('🔍 Антиплагиат','Сообщите систему проверки и требуемый процент оригинальности. Условия получения отчёта уточните у менеджера.'),
 'files':('📎 Материалы','Пришлите тему, требования, методичку и дату сдачи. Сообщения передаются менеджеру даже без заполнения анкеты.')}
log = logging.getLogger('onestudy')


def keyboard(rows):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t,callback_data=d) for t,d in row] for row in rows])


def date_value(text):
    for fmt in ('%d.%m.%Y %H:%M','%d.%m.%Y'):
        try:
            dt = datetime.strptime(text.strip(),fmt).replace(tzinfo=ZoneInfo('Europe/Moscow'))
            if dt.date() < datetime.now(ZoneInfo('Europe/Moscow')).date():
                raise ValueError('past')
            return dt.isoformat()
        except ValueError:
            continue
    raise ValueError('Укажите сегодняшнюю или будущую дату: ДД.ММ.ГГГГ, например 15.11.2026.')


def payload(m):
    """A reconstructable fallback when Telegram no longer permits copying a source."""
    d = {'text':m.text,'caption':m.caption}
    for kind in ('document','video','audio','voice','animation','sticker','video_note'):
        item = getattr(m,kind)
        if item:
            d.update(kind=kind,file=item.file_id)
            return d
    if m.photo:
        d.update(kind='photo',file=m.photo[-1].file_id)
    elif m.contact:
        d.update(kind='contact',phone_number=m.contact.phone_number,first_name=m.contact.first_name)
    elif m.location:
        d.update(kind='location',latitude=m.location.latitude,longitude=m.location.longitude)
    elif m.poll:
        d['text'] = '📊 Опрос: '+m.poll.question+'\n'+'\n'.join(o.text for o in m.poll.options)
    return d


class App:
    def __init__(self, store, bot, forum, admins, owner=None):
        self.s,self.bot,self.forum,self.admins,self.owner = store,bot,forum,set(admins),owner
        self.locks = defaultdict(asyncio.Lock)
        self.router = Router()
        r = self.router
        r.message.register(self.start,CommandStart(),F.chat.type=='private')
        r.message.register(self.private,F.chat.type=='private')
        r.message.register(self.group,F.chat.id==forum)
        r.callback_query.register(self.callback)
        r.errors.register(self.handler_error)

    async def handler_error(self,event: ErrorEvent):
        log.error('Update handler failed: %s',type(event.exception).__name__)
        await self.alert('⚠️ Ошибка обработки действия. Проверьте журнал запуска. Сохранённые сообщения остаются в очереди.')
        return True

    def menu_keys(self):
        return keyboard([[('📝 Новая заявка','new'),('📂 Мои заказы','mine:0')],
                         [('💬 Задать вопрос','talk'),('ℹ️ Частые вопросы','faq')]])

    def own(self, uid, oid):
        o=self.s.order(oid)
        if not o or o['user_id']!=uid:
            raise ValueError('Заявка недоступна')
        return o

    async def start(self,m):
        async with self.locks[m.from_user.id]:
            self.s.upsert_user(m.from_user)
            await self.show_menu(m.from_user.id)

    async def show_menu(self,uid):
        u=self.s.user(uid)
        if not u['greeted']:
            await self.bot.send_message(uid,WELCOME,reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text='🏠 Главное меню')]],resize_keyboard=True))
            self.s.execute('UPDATE users SET greeted=1 WHERE id=?',(uid,))
        await self.bot.send_message(uid,'Выберите действие 👇',reply_markup=self.menu_keys())

    async def create(self,uid,form=False):
        o=self.s.new_order(uid)
        self.s.set_order(o['id'],step=0 if form else -1)
        self.s.event(o['id'],'Заказ создан')
        # Telegram provisioning belongs to the worker; failures cannot discard the order.
        return self.s.order(o['id'])

    async def choose(self,uid):
        u=self.s.user(uid)
        if u['selector']:
            return
        orders=self.s.orders(uid)
        rows=[[(f"№{o['id']} · {(o['title'] or o['work'] or 'Заявка')[:30]}",f"select:{o['id']}")] for o in orders[:8]]
        rows.append([('🆕 Новый заказ','new')])
        msg=await self.bot.send_message(uid,'Выберите заказ для сообщения или создайте новый. Отправленные материалы сохранены до выбора.',reply_markup=keyboard(rows))
        self.s.execute('UPDATE users SET selector=? WHERE id=?',(msg.message_id,uid))

    async def ensure_topic(self,oid):
        o=self.s.order(oid)
        if not o['thread']:
            u=self.s.user(o['user_id'])
            t=await self.bot.create_forum_topic(self.forum,name=f"№{oid} · {u['name']}"[:128])
            self.s.set_order(oid,thread=t.message_thread_id)
        await self.card(oid)
        if not self.s.one('SELECT value FROM meta WHERE key=?',(f'announced:{oid}',)):
            o=self.s.order(oid)
            await self.bot.send_message(self.forum,f'🆕 Новая заявка №{oid}',message_thread_id=o['thread'])
            self.s.execute('INSERT OR IGNORE INTO meta VALUES(?,?)',(f'announced:{oid}','1'))
            self.s.event(oid,'notified')

    def card_text(self,o,client=False):
        u=self.s.user(o['user_id'])
        deadline=datetime.fromisoformat(o['deadline']).strftime('%d.%m.%Y %H:%M') if o['deadline'] else 'Не уточнён'
        text=f"<b>№{o['id']} · {STATUSES[o['status']]}</b>\n"
        if not client:
            text+=f"👤 {escape(u['name'][:70])} · {escape('@'+u['username'] if u['username'] else 'без username')}\n🆔 <code>{u['id']}</code>\n"
        for icon,key in [('📘','work'),('📚','subject'),('📝','title'),('🔍','anti')]:
            text+=f"{icon} {escape((o[key] or 'Не уточнено')[:100])}\n"
        text+=f'📅 {deadline}\n'
        text+=('💬 Ожидаем ответа менеджера' if o['waiting'] else '💬 Последнее обращение обработано')
        if o['rating']:
            text+=f"\nОценка: {o['rating']}/5"
        return text

    async def card(self,oid):
        o=self.s.order(oid)
        if not o['thread']:
            return
        text=self.card_text(o)
        markup=keyboard([[('Статус',f'status:{oid}'),('Карточка',f'card:{oid}')],
                         [('Завершить заказ',f'close:{oid}')]])
        markup.inline_keyboard.append([InlineKeyboardButton(text='👤 Профиль клиента',url=f"tg://user?id={o['user_id']}")])
        if o['card']:
            try:
                if o['photo']:
                    await self.bot.edit_message_caption(chat_id=self.forum,message_id=o['card'],caption=text,reply_markup=markup)
                else:
                    await self.bot.edit_message_text(text,chat_id=self.forum,message_id=o['card'],reply_markup=markup)
                return
            except TelegramBadRequest as e:
                if 'not modified' in str(e):
                    return
                if 'message to edit not found' not in str(e).lower():
                    raise
        photo=None
        try:
            photos=await self.bot.get_user_profile_photos(o['user_id'],limit=1)
            if photos.photos:
                photo=photos.photos[0][-1].file_id
        except (TelegramBadRequest,TelegramForbiddenError):
            pass
        opts=dict(chat_id=self.forum,message_thread_id=o['thread'],disable_notification=True,reply_markup=markup)
        if photo:
            msg=await self.bot.send_photo(photo=photo,caption=text,**opts)
        else:
            msg=await self.bot.send_message(text=text,**opts)
        self.s.set_order(oid,card=msg.message_id,photo=int(bool(photo)))
        try:
            await self.bot.pin_chat_message(self.forum,msg.message_id,disable_notification=True)
        except TelegramBadRequest:
            log.warning('Cannot pin order %s',oid)

    async def form(self,oid):
        o=self.s.order(oid)
        step=o['step']
        if step<0:
            return
        values=WORK if step==0 else SUBJECT if step==1 else ['Не знаю / уточню позже'] if step==3 else []
        rows=[[(v,f'answer:{oid}:{step}:{i}')] for i,v in enumerate(values)]
        rows.append([('⬅️ Назад',f'back:{oid}:{step}'),('Пропустить',f'skip:{oid}:{step}')])
        text=f"<b>Заявка №{oid} · Шаг {step+1} из 5</b>\n{QUESTIONS[step]}"
        if o['form_msg']:
            try:
                await self.bot.edit_message_text(text,chat_id=o['user_id'],message_id=o['form_msg'],reply_markup=keyboard(rows))
                return
            except TelegramBadRequest as e:
                if 'not modified' in str(e): return
        msg=await self.bot.send_message(o['user_id'],text,reply_markup=keyboard(rows))
        self.s.set_order(oid,form_msg=msg.message_id)

    async def advance(self,o,value):
        if o['step']<0:
            return
        field=FIELDS[o['step']]
        if value and field=='deadline':
            value=date_value(value)
        self.s.set_order(o['id'],**{field:value},step=o['step']+1 if o['step']<4 else -1)
        if o['step']==4:
            await self.bot.edit_message_text('Данные сохранены. Можно добавить материалы.',
                chat_id=o['user_id'],message_id=o['form_msg'])
            await self.receipt(o['id'])
        else:
            await self.form(o['id'])

    async def receipt(self,oid):
        o=self.s.order(oid)
        if o['receipt']:
            return
        msg=await self.bot.send_message(o['user_id'],
          f'Заявка №{oid}. Можете отправить остальные материалы сюда. Когда закончите, нажмите «Всё отправлено».',
          reply_markup=keyboard([[('Всё отправлено',f'finish:{oid}')]]))
        self.s.set_order(oid,receipt=msg.message_id)

    async def private(self,m):
        uid=m.from_user.id
        async with self.locks[uid]:
            self.s.upsert_user(m.from_user)
            u=self.s.user(uid)
            if m.text in ('🏠 Главное меню','🚀 Старт'):
                await self.show_menu(uid); return
            if u['blocked']:
                return
            if m.text and m.text.startswith('/'):
                await m.answer('Для навигации используйте /start или «🏠 Главное меню».'); return
            if self.s.one('SELECT id FROM queue WHERE src=? AND mid=?',(m.chat.id,m.message_id)):
                return
            o=self.s.order(u['selected']) if u['selected'] else None
            orders=self.s.orders(uid)
            active=[x for x in orders if x['status'] not in TERMINAL]
            if not o and len(active)==1:
                o=active[0]; self.s.select(uid,o['id'])
            if o and o['status'] in TERMINAL:
                o=None
            if not o and not orders:
                o=await self.create(uid)
            accepted=self.s.enqueue(m,o['id'] if o else None,'in',payload(m))
            if not accepted: return
            if not o:
                await self.choose(uid); return
            if o['review']:
                self.s.set_order(o['id'],review=0)
                self.s.event(o['id'],'Клиент оставил обратную связь')
            if o['step']>=0 and m.text:
                try:
                    await self.advance(o,m.text[:500])
                except ValueError as e:
                    await m.answer(str(e))
            await self.receipt(o['id'])

    async def group(self,m):
        if not m.from_user or m.from_user.id not in self.admins or m.sender_chat:
            return
        text=m.text or ''
        cmd=text.split()[0].split('@')[0] if text else ''
        if cmd in ('/clients','/orders'):
            await self.dashboard(m.chat.id,'all',0); return
        if cmd=='/search':
            query=text.partition(' ')[2].strip()
            await self.search(m,query); return
        if cmd=='/retry':
            o=self.s.one('SELECT * FROM orders WHERE thread=?',(m.message_thread_id,))
            if o:
                self.s.execute("UPDATE queue SET state='pending',due=0,tries=0,alerted=0 WHERE oid=? AND state='failed'",(o['id'],))
                notice=self.s.one('SELECT value FROM meta WHERE key=?',(f'failed_close:{o["id"]}',))
                if notice:
                    with self.s.c:
                        self.s.c.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(f'close:{o["id"]}',notice['value']))
                        self.s.c.execute('DELETE FROM meta WHERE key=?',(f'failed_close:{o["id"]}',))
                await m.answer('Повторная отправка запланирована.',disable_notification=True)
            return
        o=self.s.one('SELECT * FROM orders WHERE thread=?',(m.message_thread_id,))
        if not o: return
        if cmd=='/note':
            self.s.event(o['id'],'Заметка: '+text.partition(' ')[2]); return
        if cmd in ('/block','/unblock'):
            self.s.execute('UPDATE users SET blocked=? WHERE id=?',(int(cmd=='/block'),o['user_id']))
            await m.answer('Доступ обновлён.',disable_notification=True); return
        if cmd=='/done':
            await m.answer('Подтвердите завершение заказа.',reply_markup=keyboard([[('Завершить',f'confirm:{o["id"]}'),('Отмена',f'card:{o["id"]}')]]),disable_notification=True); return
        if cmd.startswith('/') or m.content_type not in ('text','photo','document','video','audio','voice','animation','sticker','video_note','contact','location','poll'):
            return
        self.s.enqueue(m,o['id'],'out',payload(m))

    async def search(self,m,query):
        if not query:
            await m.answer('Пример: /search Анна или /search 1042',disable_notification=True); return
        term='%'+query.replace('%','').replace('_','')+'%'
        rows=self.s.all('''SELECT o.* FROM orders o JOIN users u ON o.user_id=u.id
          WHERE CAST(o.id AS TEXT)=? OR CAST(u.id AS TEXT)=? OR u.name LIKE ?
          OR u.username LIKE ? ORDER BY o.id DESC LIMIT 20''',(query,query,term,term))
        await self.send_order_list(m.chat.id,rows,'Результаты поиска')

    async def send_order_list(self,chat,orders,title,extra=None,edit=None):
        rows=[]
        for o in orders:
            if o['thread']:
                url=f"https://t.me/c/{str(self.forum).removeprefix('-100')}/{o['thread']}"
                u=self.s.user(o['user_id'])
                wait=f" · ждёт {int((time.time()-o['waiting'])/60)} мин" if o['waiting'] else ''
                rows.append([InlineKeyboardButton(text=f"№{o['id']} · {u['name'][:22]}{wait}",url=url)])
        if extra: rows.extend(extra)
        text=title if orders else title+'\nЗаказов нет.'
        markup=InlineKeyboardMarkup(inline_keyboard=rows)
        if edit:
            try: await edit.edit_text(text,reply_markup=markup)
            except TelegramBadRequest as e:
                if 'not modified' not in str(e): raise
        else:
            await self.bot.send_message(chat,text,reply_markup=markup,disable_notification=True)

    async def dashboard(self,chat,kind,page,edit=None):
        clauses={'all':"status NOT IN ('done','cancelled','archive')",'wait':"waiting IS NOT NULL AND status NOT IN ('done','cancelled','archive')",
         'new':"status='new'",'pay':"status='payment'",'client':"waiting IS NULL AND status NOT IN ('done','cancelled','archive')",
         'soon':"deadline!='' AND status NOT IN ('done','cancelled','archive')"}
        if kind not in clauses: raise ValueError('Неизвестный раздел')
        rows=self.s.all('SELECT * FROM orders WHERE '+clauses[kind]+(' ORDER BY deadline,id' if kind=='soon' else ' ORDER BY COALESCE(waiting,created),id')+' LIMIT 11 OFFSET ?',(page*10,))
        nav=keyboard([[('🔴 Ждут ответа','dash:wait:0'),('🆕 Новые','dash:new:0')],
          [('⏰ Сроки','dash:soon:0'),('💳 Оплата','dash:pay:0')],
          [('💬 Ждём клиента','dash:client:0'),('📂 Все','dash:all:0')]]).inline_keyboard
        pages=[]
        if page: pages.append(InlineKeyboardButton(text='⬅️',callback_data=f'dash:{kind}:{page-1}'))
        if len(rows)>10: pages.append(InlineKeyboardButton(text='➡️',callback_data=f'dash:{kind}:{page+1}'))
        if pages: nav.append(pages)
        await self.send_order_list(chat,rows[:10],f'📋 Заказы · страница {page+1}\nПоиск: /search имя или номер',nav,edit)

    async def callback(self,c):
        uid=c.from_user.id
        try:
            async with self.locks[uid]:
                await self.dispatch_callback(c)
        except ValueError as e:
            await c.answer(str(e)[:180],show_alert=True)
        except (TelegramBadRequest,TelegramForbiddenError):
            log.exception('Callback failed')
            await c.answer('Действие не выполнено. Откройте меню и повторите.',show_alert=True)

    async def dispatch_callback(self,c):
        data=c.data or ''
        uid=c.from_user.id
        private=c.message.chat.type=='private'
        if not private:
            if c.message.chat.id!=self.forum or uid not in self.admins:
                raise ValueError('Доступ только менеджеру')
            await c.answer()
            await self.admin_callback(c,data); return
        self.s.upsert_user(c.from_user)
        if self.s.user(uid)['blocked']:
            raise ValueError('Доступ ограничен. Свяжитесь с OneStudy другим способом.')
        await c.answer()
        if data=='new':
            # Old menu presses do not create duplicates while an empty form is open.
            old=self.s.order(self.s.user(uid)['selected'])
            o=old if old and old['step']>=0 else await self.create(uid,True)
            await self.form(o['id'])
        elif data=='talk':
            orders=self.s.orders(uid)
            if not orders: await self.create(uid)
            elif len(orders)>1 or orders[0]['status'] in TERMINAL: await self.choose(uid)
            await self.bot.send_message(uid,'Напишите вопрос или отправьте материалы сюда.')
        elif data.startswith('mine:'):
            page=max(0,int(data.split(':')[1]))
            orders=self.s.orders(uid)[page*8:page*8+9]
            rows=[[(f"№{o['id']} · {STATUSES[o['status']]}",f'view:{o["id"]}')] for o in orders[:8]]
            if page: rows.append([('⬅️ Назад',f'mine:{page-1}')])
            if len(orders)>8: rows.append([('Далее ➡️',f'mine:{page+1}')])
            await self.bot.send_message(uid,'📂 Мои заказы' if orders else 'Пока заказов нет.',reply_markup=keyboard(rows))
        elif data.startswith('view:'):
            o=self.own(uid,int(data.split(':')[1]))
            await self.bot.send_message(uid,self.card_text(o,True),reply_markup=keyboard([[('💬 Написать / добавить материалы',f'select:{o["id"]}')]]))
        elif data.startswith('select:'):
            oid=int(data.split(':')[1]); self.own(uid,oid)
            self.s.select(uid,oid)
            await c.message.edit_text(f'Выбран заказ №{oid}. Сообщения будут направляться в него.')
            await self.receipt(oid)
        elif data.startswith(('answer:','skip:','back:')):
            parts=data.split(':'); oid,step=int(parts[1]),int(parts[2]); o=self.own(uid,oid)
            if o['step']!=step or o['status'] in TERMINAL:
                raise ValueError('Этот шаг уже изменился. Откройте актуальную анкету.')
            if parts[0]=='back':
                self.s.set_order(oid,step=max(0,step-1)); await self.form(oid)
            else:
                options=WORK if step==0 else SUBJECT if step==1 else ['Не знаю / уточню позже']
                value=options[int(parts[3])] if parts[0]=='answer' else ''
                await self.advance(o,value)
        elif data.startswith('finish:'):
            oid=int(data.split(':')[1]); o=self.own(uid,oid)
            if o['status'] in TERMINAL: raise ValueError('Заказ уже завершён')
            if not o['fact']:
                self.s.set_order(oid,fact=self.s.fact(uid,FACTS))
            if not o['receipt']:
                await self.receipt(oid)
            if o['finish']: return
            self.s.set_order(oid,finish=1)
            await c.message.edit_text('Передаём материалы…')
        elif data.startswith('rate:'):
            _,oid,rating=data.split(':'); o=self.own(uid,int(oid)); rating=int(rating)
            if rating not in range(1,6) or o['status']!='done': raise ValueError('Оценка сейчас недоступна')
            if o['rating'] is not None: raise ValueError('Оценка уже сохранена')
            self.s.set_order(o['id'],rating=rating)
            await c.message.edit_text(f'Спасибо за оценку! {rating}/5.',reply_markup=keyboard([[('Оставить отзыв / рассказать о проблеме',f'review:{oid}')]]))
            await self.card(o['id'])
        elif data.startswith('review:'):
            oid=int(data.split(':')[1]); self.own(uid,oid); self.s.select(uid,oid)
            self.s.set_order(oid,review=1)
            await self.bot.send_message(uid,'Напишите отзыв или расскажите о проблеме. Сообщение получит менеджер.')
        elif data=='faq':
            await c.message.edit_text('Частые вопросы',reply_markup=keyboard([[(v[0],'faq:'+k)] for k,v in FAQ.items()]))
        elif data.startswith('faq:'):
            key=data.split(':')[1]
            if key not in FAQ: raise ValueError('Вопрос не найден')
            await c.message.edit_text(FAQ[key][1],reply_markup=keyboard([[('⬅️ К вопросам','faq')]]))

    async def admin_callback(self,c,data):
        if data.startswith('dash:'):
            _,kind,page=data.split(':'); await self.dashboard(self.forum,kind,max(0,int(page)),c.message); return
        parts=data.split(':')
        if len(parts)<2 or not parts[1].isdigit(): return
        oid=int(parts[1]); o=self.s.order(oid)
        if not o: raise ValueError('Заказ не найден')
        if parts[0]=='status':
            await c.message.reply('Выберите статус:',reply_markup=keyboard([[(v,f'set:{oid}:{k}')] for k,v in STATUSES.items() if k!='done']),disable_notification=True)
        elif parts[0]=='set':
            status=parts[2]
            if status not in STATUSES or status=='done': raise ValueError('Недопустимый статус')
            self.s.set_order(oid,status=status,activity=time.time(),warned=0)
            self.s.event(oid,'Статус: '+status)
            await self.card(oid)
        elif parts[0]=='card':
            await self.card(oid)
            history=self.s.all('SELECT action,created FROM events WHERE oid=? AND action!=? ORDER BY id DESC LIMIT 8',(oid,'notified'))
            details=self.card_text(self.s.order(oid))+'\n\n<b>История</b>\n'+'\n'.join(escape(r['action'][:200]) for r in history)
            await c.message.reply(details,disable_notification=True)
        elif parts[0]=='close':
            await c.message.reply(f'Завершить заказ №{oid}?',reply_markup=keyboard([[('Завершить',f'confirm:{oid}'),('Отмена',f'cancelclose:{oid}')]]),disable_notification=True)
        elif parts[0]=='cancelclose':
            await c.message.edit_text('Завершение отменено.')
        elif parts[0]=='confirm':
            if o['status']=='done': return
            if self.s.one("SELECT id FROM queue WHERE oid=? AND state!='sent' LIMIT 1",(oid,)):
                raise ValueError('Сначала дождитесь передачи сообщений или исправьте ошибку через /retry.')
            # Commit status and outgoing notice together, before the Telegram call.
            with self.s.c:
                self.s.c.execute("UPDATE orders SET status='done',step=-1,waiting=NULL,finish=0 WHERE id=?",(oid,))
                self.s.c.execute('INSERT INTO events(oid,action,created) VALUES(?,?,?)',(oid,'Завершён менеджером',time.time()))
                self.s.c.execute('INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)',
                    (f'close:{oid}',json.dumps({'uid':o['user_id'],'due':0,'tries':0})))
            await c.message.edit_text(f'Заказ №{oid} завершён. Уведомление клиенту поставлено в очередь.')
            await self.card(oid)

    async def fallback_send(self,row,target,thread,silent):
        d=json.loads(row['payload']); kind=d.get('kind'); opts=dict(chat_id=target,disable_notification=silent)
        if thread: opts['message_thread_id']=thread
        if kind in ('photo','document','video','audio','voice','animation'):
            return await getattr(self.bot,'send_'+kind)(**opts,**{kind:d['file']},caption=d.get('caption'),parse_mode=None)
        if kind in ('sticker','video_note'):
            return await getattr(self.bot,'send_'+kind)(**opts,**{kind:d['file']})
        if kind=='contact': return await self.bot.send_contact(**opts,phone_number=d['phone_number'],first_name=d['first_name'])
        if kind=='location': return await self.bot.send_location(**opts,latitude=d['latitude'],longitude=d['longitude'])
        if not d.get('text'): raise ValueError('Неподдерживаемый тип сообщения')
        return await self.bot.send_message(**opts,text=d['text'],parse_mode=None)

    async def alert(self,text,oid=None):
        o=self.s.order(oid) if oid else None
        try:
            await self.bot.send_message(self.owner or self.forum,text,
                message_thread_id=o['thread'] if o and not self.owner else None)
        except Exception:
            log.error('Unable to deliver admin alert')

    async def deliver(self,rows):
        r=rows[0]; o=self.s.order(r['oid'])
        if not o['thread']: await self.ensure_topic(o['id']); o=self.s.order(o['id'])
        inbound=r['direction']=='in'
        target=self.forum if inbound else o['user_id']; thread=o['thread'] if inbound else None
        last=self.s.one('SELECT created FROM events WHERE oid=? AND action=? ORDER BY id DESC LIMIT 1',(o['id'],'notified'))
        silent=inbound and bool(last and (o['reminded'] or (o['waiting'] and last['created']>=o['waiting']) or time.time()-last['created']<60))
        for row in rows: self.s.execute("UPDATE queue SET state='sending' WHERE id=?",(row['id'],))
        opts=dict(chat_id=target,from_chat_id=r['src'],disable_notification=silent)
        if thread: opts['message_thread_id']=thread
        if len(rows)>1:
            media_types={'photo':InputMediaPhoto,'video':InputMediaVideo,
                         'document':InputMediaDocument,'audio':InputMediaAudio}
            saved=[json.loads(row['payload']) for row in rows]
            if all(d.get('kind') in media_types for d in saved):
                media=[media_types[d['kind']](media=d['file'],caption=d.get('caption'),parse_mode=None) for d in saved]
                send_opts={k:v for k,v in opts.items() if k!='from_chat_id'}
                sent=await self.bot.send_media_group(**send_opts,media=media)
            else:
                sent=await self.bot.copy_messages(**opts,message_ids=[x['mid'] for x in rows])
            if len(sent)!=len(rows):
                raise ValueError('Telegram пропустил часть альбома; требуется проверка менеджера')
        else:
            try: await self.bot.copy_message(**opts,message_id=r['mid'])
            except TelegramBadRequest as e:
                if not any(x in str(e).lower() for x in ('message to copy not found',"can't be copied",'message_id_invalid')): raise
                await self.fallback_send(r,target,thread,silent)
        for row in rows: self.s.execute("UPDATE queue SET state='sent',error=NULL WHERE id=?",(row['id'],))
        if inbound:
            if not silent: self.s.event(o['id'],'notified')
        else:
            # Messages received after this reply was queued still need an answer.
            later=self.s.one("SELECT MIN(created) t FROM queue WHERE oid=? AND direction='in' AND created>?",(o['id'],max(x['created'] for x in rows)))['t']
            self.s.set_order(o['id'],waiting=later,reminded=0,activity=time.time())
        await self.card(o['id'])

    async def tick(self):
        for o in self.s.all('SELECT * FROM orders WHERE thread IS NULL LIMIT 5'):
            try: await self.ensure_topic(o['id'])
            except Exception:
                log.exception('Topic creation delayed for order %s',o['id'])
        rows=self.s.next_batch()
        if rows:
            try: await self.deliver(rows)
            except Exception as e:
                permanent=isinstance(e,(TelegramForbiddenError,ValueError))
                tries=rows[0]['tries']+1
                failed=permanent or tries>=8
                delay=e.retry_after+1 if isinstance(e,TelegramRetryAfter) else min(300,2**tries)
                for row in rows:
                    # A card refresh failure after delivery must never resend the content.
                    if self.s.one('SELECT state FROM queue WHERE id=?',(row['id'],))['state']=='sent': continue
                    self.s.execute('UPDATE queue SET state=?,tries=?,due=?,error=? WHERE id=?',
                        ('failed' if failed else 'pending',tries,time.time()+delay,type(e).__name__,row['id']))
                if not rows[0]['alerted']:
                    self.s.execute('UPDATE queue SET alerted=1 WHERE id=?',(rows[0]['id'],))
                    await self.alert(f"⚠️ Задержка передачи по заказу №{rows[0]['oid']}. Проверьте очередь командой /retry в теме. Ошибка: {type(e).__name__}",rows[0]['oid'])
                    if rows[0]['direction']=='in':
                        try: await self.bot.send_message(self.s.order(rows[0]['oid'])['user_id'],'Передача материалов задерживается. Они сохранены, менеджер уведомлён.',disable_notification=True)
                        except Exception: pass
        for o in self.s.all('SELECT * FROM orders WHERE finish=1'):
            if self.s.pending(o['id']): continue
            try:
                await self.bot.edit_message_text(f"Материалы по заказу №{o['id']} переданы менеджеру ✅\nОтвет придёт сюда. Дополнения можно отправлять в этот чат.\n\n💡 Учебная минутка\n{escape(o['fact'] or '')}",
                    chat_id=o['user_id'],message_id=o['receipt'])
            except TelegramBadRequest as e:
                if 'not modified' not in str(e):
                    await self.alert(f"Не удалось обновить подтверждение заказа №{o['id']}",o['id'])
            except TelegramForbiddenError:
                await self.alert(f"Клиент заказа №{o['id']} недоступен для подтверждения.",o['id'])
            self.s.set_order(o['id'],finish=0,batch=0)
        for item in self.s.all("SELECT * FROM meta WHERE key LIKE 'close:%' LIMIT 5"):
            oid=int(item['key'].split(':')[1]); o=self.s.order(oid)
            notice=json.loads(item['value'])
            if notice['due']>time.time(): continue
            if o['status']!='done':
                self.s.execute('DELETE FROM meta WHERE key=?',(item['key'],)); continue
            try:
                await self.bot.send_message(notice['uid'],f'Заказ №{oid} завершён. Спасибо за обращение!\nОцените, пожалуйста, как прошла работа.',
                    reply_markup=keyboard([[(f'{n}/5',f'rate:{oid}:{n}') for n in range(1,6)]]))
            except TelegramForbiddenError:
                await self.alert(f'Клиент заказа №{oid} заблокировал бота. Уведомление о завершении не доставлено.',oid)
                self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(f'failed_close:{oid}',item['value']))
            except TelegramBadRequest:
                await self.alert(f'Не удалось отправить завершение заказа №{oid}. После устранения причины используйте /retry.',oid)
                self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(f'failed_close:{oid}',item['value']))
            except (TelegramRetryAfter,TelegramNetworkError) as e:
                notice['tries']+=1
                notice['due']=time.time()+(e.retry_after+1 if isinstance(e,TelegramRetryAfter) else min(300,2**min(notice['tries'],9)))
                self.s.execute('UPDATE meta SET value=? WHERE key=?',(json.dumps(notice),item['key']))
                continue
            self.s.execute('DELETE FROM meta WHERE key=?',(item['key'],))

    async def maintenance(self):
        now=time.time()
        for o in self.s.all("SELECT * FROM orders WHERE status NOT IN ('done','cancelled','archive')"):
            if o['thread'] and o['waiting'] and now-o['waiting']>=1800 and not o['reminded']:
                await self.bot.send_message(self.forum,f"⏰ Клиент по заказу №{o['id']} ждёт ответа.",message_thread_id=o['thread'])
                self.s.set_order(o['id'],reminded=1)
            days=(now-o['activity'])/86400
            if days>=30 and o['status'] in ('new','clarify') and not self.s.pending(o['id']):
                self.s.set_order(o['id'],status='archive'); self.s.event(o['id'],'Архив по неактивности'); await self.card(o['id'])
            elif days>=7 and not o['warned'] and o['thread']:
                await self.bot.send_message(self.forum,'⚠️ Нет переписки 7 дней. Проверьте состояние заказа.',message_thread_id=o['thread'],disable_notification=True)
                self.s.set_order(o['id'],warned=1)

    async def guide(self):
        if self.s.one("SELECT value FROM meta WHERE key='guide_v3'"): return
        t=await self.bot.create_forum_topic(self.forum,name='📖 OneStudy · инструкция v3')
        msg=await self.bot.send_message(self.forum,
          '<b>OneStudy v3 · менеджеру</b>\n\n'
          '/orders или /clients — список заказов.\n'
          '/search Анна — поиск по имени; также можно указать номер или Telegram ID.\n'
          '/done — завершение с подтверждением в теме заказа.\n'
          '/note текст — внутренняя заметка, клиент её не получит.\n'
          '/retry — повторить неудачные отправки в этой теме.\n'
          '/block и /unblock — ограничить или восстановить приём сообщений клиента.\n\n'
          '<b>Обычные сообщения в клиентской теме уходят клиенту.</b>\n'
          'Статусы меняются кнопкой карточки. Новые заказы не заменяют старые.\n'
          'Напоминание через 30 минут действует круглосуточно: график пока не настроен.\n'
          'Рабочие заказы автоматически не закрываются.\n'
          'При ошибке доставки проверьте права и доступность клиента, затем используйте /retry.\n'
          'При частично переданном альбоме сначала проверьте тему: повтор может создать дубли.',
          message_thread_id=t.message_thread_id,disable_notification=True,
          reply_markup=keyboard([[('📋 Заказы','dash:all:0')]]))
        await self.bot.pin_chat_message(self.forum,msg.message_id,disable_notification=True)
        self.s.execute("INSERT INTO meta VALUES('guide_v3',?)",(str(t.message_thread_id),))

    async def worker(self):
        last_maintenance=0; last_backup=0
        while True:
            try:
                await self.tick()
                now=time.time()
                if now-last_maintenance>60:
                    await self.maintenance(); last_maintenance=now
                if now-last_backup>86400:
                    path=self.s.backup(self.s.path.parent/'backups')
                    if self.owner:
                        await self.bot.send_document(self.owner,FSInputFile(path),caption='Резервная копия OneStudy. Содержит данные клиентов.',disable_notification=True)
                    last_backup=now
            except asyncio.CancelledError: raise
            except Exception:
                log.exception('Worker cycle failed')
            await asyncio.sleep(1.2)


async def main():
    logging.basicConfig(level=logging.INFO)
    token=os.environ.get('BOT_TOKEN'); forum=int(os.environ.get('FORUM_CHAT_ID','0'))
    admins={int(x.strip()) for x in os.environ.get('ADMIN_IDS','').split(',') if x.strip()}
    if not token or not forum or not admins:
        raise RuntimeError('Нужны BOT_TOKEN, FORUM_CHAT_ID и ADMIN_IDS (Telegram ID менеджера).')
    folder=Path(os.environ.get('DATA_DIR','./data')); folder.mkdir(parents=True,exist_ok=True)
    # Prevent a second local instance from racing the queue or SQLite migrations.
    import fcntl
    lock=open(folder/'bot.lock','w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    store=Store(folder/'threads.db')
    bot=Bot(token,default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    worker=None
    try:
        me=await bot.get_me(); chat=await bot.get_chat(forum)
        member=await bot.get_chat_member(forum,me.id)
        if not chat.is_forum or member.status!='administrator' or not member.can_manage_topics or not member.can_pin_messages:
            raise RuntimeError('Бот должен быть администратором группы с темами, управлением темами и закреплением сообщений.')
        hook=await bot.get_webhook_info()
        if hook.url:
            raise RuntimeError('У бота настроен webhook. Удалите его перед запуском long polling, сохранив ожидающие обновления.')
        await bot.set_my_commands([BotCommand(command='start',description='Главное меню')])
        owner=int(os.environ.get('OWNER_CHAT_ID','0')) or None
        app=App(store,bot,forum,admins,owner)
        try: await app.guide()
        except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError):
            log.warning('Не удалось создать служебную инструкцию; см. README.md')
        dp=Dispatcher(); dp.include_router(app.router)
        worker=asyncio.create_task(app.worker())
        # Sequential update handling: persist every message before requesting more updates.
        await dp.start_polling(bot,handle_as_tasks=False,allowed_updates=['message','callback_query'],close_bot_session=False)
    finally:
        if worker:
            worker.cancel()
            try: await worker
            except asyncio.CancelledError: pass
        store.c.close(); await bot.session.close(); lock.close()


if __name__=='__main__':
    asyncio.run(main())
