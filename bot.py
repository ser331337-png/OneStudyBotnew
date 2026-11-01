"""OneStudy v3.4: one process, persistent inbox/outbox and multiple orders."""
from __future__ import annotations
import asyncio
from collections import defaultdict, deque
from contextvars import ContextVar
from datetime import datetime, timedelta
import calendar
from html import escape
import json
import logging
import math
import os
import re
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
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio, LinkPreviewOptions
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
ANTI=['Да','Нет','Не знаю']


def quick_dates(today=None):
    today=today or datetime.now(ZoneInfo('Europe/Moscow')).date()
    result=[(name,today+timedelta(days=n)) for name,n in
            [('Сегодня',0),('Завтра',1),('Через 3 дня',3),('Через неделю',7),('Через 2 недели',14)]]
    year=today.year+(today.month==12); month=today.month%12+1
    result.append(('Через месяц',today.replace(year=year,month=month,
                  day=min(today.day,calendar.monthrange(year,month)[1]))))
    return result
WELCOME = 'Здравствуйте! Мы OneStudy, всегда готовы проконсультировать по учебным вопросам!\nВыберите, какая помощь нужна, или сразу напишите вопрос и прикрепите документы (при наличии).'
FAQ = {'pay':('💳 Оплата','50% предоплата. По мере готовности показываем фрагменты работы. После согласования и оплаты остатка передаём итоговые файлы.'),
 'fix':('🛡 Доработки','Правки в рамках исходного задания — бесплатно. Новые требования и дополнительный объём обсуждаются отдельно.'),
 'anti':('🔍 Антиплагиат','Сообщите систему проверки и требуемый процент оригинальности. Условия получения отчёта уточните у менеджера.'),
 'files':('📎 Материалы','Пришлите тему, требования, методичку и дату сдачи. Сообщения передаются менеджеру даже без заполнения анкеты.')}
log = logging.getLogger('onestudy')
BACKGROUND_REQUEST = ContextVar('onestudy_background_request',default=False)


class RateGuard:
    """Retry only explicit 429 rejections; never replay ambiguous network failures.

    One chat budget covers all forum topics. Long background cooldowns are
    deferred to durable jobs so the worker can continue with other chats.
    """
    def __init__(self,store,clock=time.time,sleep=asyncio.sleep):
        self.s,self.clock,self.sleep=store,clock,sleep
        self.locks=defaultdict(asyncio.Lock)
        self.next_call=defaultdict(float)
        self.messages=defaultdict(deque)
        self.global_lock=asyncio.Lock()
        self.global_next=0

    def cooldown(self,scope):
        until=0
        for key in {str(scope),'bot'}:
            row=self.s.one('SELECT value FROM meta WHERE key=?',('tg_pause:'+key,))
            if row: until=max(until,float(row['value']))
        return max(0,until-self.clock())

    def pause(self,scope,seconds):
        key='tg_pause:'+str(scope)
        old=self.s.one('SELECT value FROM meta WHERE key=?',(key,))
        due=max(float(old['value']) if old else 0,self.clock()+seconds+1)
        self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(key,str(due)))

    async def __call__(self,make_request,bot,method):
        name=method.__api_method__
        if name=='getUpdates': return await make_request(bot,method)
        chat=getattr(method,'chat_id',None)
        scope=str(chat) if chat is not None else 'bot'
        group=chat is not None and (not isinstance(chat,int) or chat<0)
        output=name.startswith(('send','copy','forward')) and name!='sendChatAction'
        units=len(getattr(method,'media',None) or getattr(method,'message_ids',None) or [1]) if output else 0
        if BACKGROUND_REQUEST.get() and self.locks[scope].locked():
            raise TelegramRetryAfter(method=method,message='Chat request in progress',retry_after=max(1,math.ceil(self.cooldown(scope))))
        async with self.locks[scope]:
            while True:
                pause=self.cooldown(scope)
                if pause and BACKGROUND_REQUEST.get():
                    raise TelegramRetryAfter(method=method,message='Stored Telegram cooldown',retry_after=math.ceil(pause))
                now=self.clock(); wait=max(pause,self.next_call[scope]-now,0)
                history=self.messages[scope]
                while history and history[0][0]<=now-61: history.popleft()
                if group and units:
                    count=sum(n for _,n in history)
                    for when,n in history:
                        if count+units<=20: break
                        wait=max(wait,when+61-now); count-=n
                if wait>0:
                    if BACKGROUND_REQUEST.get() and wait>1:
                        raise TelegramRetryAfter(method=method,message='Local request budget',retry_after=math.ceil(wait))
                    await self.sleep(min(wait,30)); continue
                async with self.global_lock:
                    wait=self.global_next-self.clock()
                    if wait>0: await self.sleep(wait)
                    self.global_next=self.clock()+0.04
                interval=3.1*units if group and output else 1.05 if chat is not None and not group and (output or name.startswith('editMessage')) else 0.1
                self.next_call[scope]=self.clock()+interval
                try:
                    result=await make_request(bot,method)
                except TelegramRetryAfter as e:
                    self.pause(scope,e.retry_after)
                    log.info('Telegram cooldown: method=%s chat=%s seconds=%s',name,scope,e.retry_after)
                    if BACKGROUND_REQUEST.get(): raise
                    continue
                if group and units: history.append((self.clock(),units))
                return result


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
    def __init__(self, store, bot, forum, admins, owner=None, reviews_chat=None, rate_guard=None):
        self.s,self.bot,self.forum,self.admins,self.owner = store,bot,forum,set(admins),owner
        self.reviews_chat=reviews_chat
        self.rate_guard=rate_guard
        if reviews_chat and reviews_chat==forum:
            raise ValueError('Группа отзывов должна отличаться от рабочей группы.')
        self.locks = defaultdict(asyncio.Lock)
        self.router = Router()
        r = self.router
        r.message.register(self.start,CommandStart(),F.chat.type=='private')
        r.message.register(self.private,F.chat.type=='private')
        r.message.register(self.group,F.chat.id==forum)
        r.callback_query.register(self.callback)
        r.errors.register(self.handler_error)

    async def handler_error(self,event: ErrorEvent):
        if isinstance(event.exception,TelegramRetryAfter):
            log.info('Action deferred by Telegram for %s seconds',event.exception.retry_after)
            return True
        log.error('Update handler failed: %s',type(event.exception).__name__)
        await self.alert('⚠️ Ошибка обработки действия. Проверьте журнал запуска. Сохранённые сообщения остаются в очереди.')
        return True

    def ready_jobs(self,prefix,limit=3):
        jobs=[]
        for row in self.s.all('SELECT * FROM meta WHERE key LIKE ?',(prefix+':%',)):
            try: data=json.loads(row['value'])
            except (ValueError,TypeError): data={}
            if not isinstance(data,dict): data={}
            if data.get('due',0)<=time.time(): jobs.append(row)
            if len(jobs)>=limit: break
        return jobs

    def defer_job(self,key,error):
        old=self.s.one('SELECT value FROM meta WHERE key=?',(key,))
        try: job=json.loads(old['value']) if old else {}
        except (ValueError,TypeError): job={}
        if not isinstance(job,dict): job={}
        tries=job.get('tries',0)+1
        delay=error.retry_after+1 if isinstance(error,TelegramRetryAfter) else 300 if isinstance(error,TelegramForbiddenError) else min(300,2**min(tries,9))
        self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',
            (key,json.dumps({'due':time.time()+delay,'tries':tries})))
        if not isinstance(error,TelegramRetryAfter) and tries==1:
            log.warning('Background job deferred: %s (%s)',key,type(error).__name__)

    async def answer_callback(self,c,*args,**kwargs):
        try: await c.answer(*args,**kwargs)
        except TelegramBadRequest as e:
            if not any(t in str(e).lower() for t in ('query is too old','query id is invalid','query_id_invalid')): raise

    def topic_name(self,o):
        u=self.s.user(o['user_id'])
        return f"{self.label(o)} · {u['name'][:40]} · {o['work'][:30]}"[:128]

    def menu_keys(self):
        return keyboard([[('📝 Новая заявка','new'),('📂 Мои заказы','mine:0')],
                         [('💬 Задать вопрос','talk'),('ℹ️ Частые вопросы','faq')]])

    def label(self,o):
        return 'Заказ №'+o['number'] if o.get('number') else 'Заявка от '+datetime.fromtimestamp(o['created'],ZoneInfo('Europe/Moscow')).strftime('%d.%m %H:%M')

    def unavailable(self,o):
        item=self.s.one('SELECT value FROM meta WHERE key=?',(f'unavailable:{o["id"]}',))
        return bool(item and item['value']==f"{self.forum}:{o['thread']}")

    async def missing_topic(self,o):
        if self.unavailable(o): return
        self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',
                       (f'unavailable:{o["id"]}',f"{self.forum}:{o['thread']}"))
        self.s.execute('DELETE FROM meta WHERE key=?',(f'rename:{o["id"]}',))
        log.warning('Missing topic recorded for internal order %s',o['id'])
        # Do not post into the missing topic. Notify only in an available chat.
        try:
            await self.bot.send_message(self.owner or self.forum,
              f'⚠️ Недоступна переписка: {self.label(o)}. Проверьте FORUM_CHAT_ID. '
              'Тестовую заявку можно закрыть в архив через /orders. Данные сохранены.',disable_notification=True)
        except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter):
            log.warning('Missing-topic notice could not be delivered')

    @staticmethod
    def is_missing_topic(error):
        return isinstance(error,TelegramBadRequest) and any(t in str(error).lower() for t in
            ('message thread not found','topic_id_invalid','topic not found'))

    async def remove(self,chat,mid):
        if not mid: return
        try:
            await self.bot.delete_message(chat,mid)
        except TelegramBadRequest:
            # Old Telegram messages may be too old to delete. Disable their buttons.
            try: await self.bot.edit_message_reply_markup(chat_id=chat,message_id=mid,reply_markup=None)
            except TelegramBadRequest: pass

    async def clear_screen(self,uid):
        screen=self.s.one('SELECT mid FROM screens WHERE uid=?',(uid,))
        if screen:
            await self.remove(uid,screen['mid'])
        # Pause form UI without losing its answers or current step.
        for o in self.s.orders(uid):
            if o['form_msg']:
                await self.remove(uid,o['form_msg'])
                self.s.set_order(o['id'],form_msg=None)
        self.s.execute('DELETE FROM screens WHERE uid=?',(uid,))
        self.s.execute('UPDATE users SET selector=NULL WHERE id=?',(uid,))

    async def screen(self,uid,text,markup=None):
        await self.clear_screen(uid)
        m=await self.bot.send_message(uid,text,reply_markup=markup)
        self.s.execute('INSERT OR REPLACE INTO screens VALUES(?,?)',(uid,m.message_id))
        return m

    async def consume(self,c,action):
        mid=getattr(c.message,'message_id',None)
        if isinstance(mid,int) and not self.s.claim(c.message.chat.id,mid,action):
            raise ValueError('Это действие уже выполнено. Откройте главное меню.')

    def current(self,uid,oid):
        o=self.own(uid,oid)
        if self.s.user(uid)['selected']!=oid:
            raise ValueError('Кнопка относится к другому заказу. Выберите его в «Мои заказы».')
        return o

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
        self.s.execute('DELETE FROM review_sessions WHERE uid=?',(uid,))
        u=self.s.user(uid)
        if not u['greeted']:
            await self.bot.send_message(uid,WELCOME,reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text='🏠 Главное меню')]],resize_keyboard=True))
            self.s.execute('UPDATE users SET greeted=1 WHERE id=?',(uid,))
        await self.screen(uid,'Выберите действие 👇',self.menu_keys())

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
        rows=[[(f"{self.label(o)} · {(o['title'] or o['work'] or 'Заявка')[:30]}",f"select:{o['id']}")] for o in orders[:8]]
        rows.append([('🆕 Новый заказ','new')])
        msg=await self.screen(uid,'Выберите заказ для сообщения или создайте новый. Отправленные материалы сохранены до выбора.',keyboard(rows))
        self.s.execute('UPDATE users SET selector=? WHERE id=?',(msg.message_id,uid))

    async def ensure_topic(self,oid):
        o=self.s.order(oid)
        if self.unavailable(o): return
        if not o['thread']:
            self.s.execute('INSERT OR IGNORE INTO meta VALUES(?,?)',(f'provision:{oid}','1'))
            name=self.topic_name(o)
            t=await self.bot.create_forum_topic(self.forum,name=name)
            self.s.set_order(oid,thread=t.message_thread_id)
            self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',
                (f'topicname:{oid}',json.dumps([self.forum,t.message_thread_id,name])))
        if not self.s.order(oid)['card']: await self.card(oid)
        if not self.s.one('SELECT value FROM meta WHERE key=?',(f'announced:{oid}',)):
            o=self.s.order(oid)
            await self.bot.send_message(self.forum,f'🆕 {self.label(o)}',message_thread_id=o['thread'])
            self.s.execute('INSERT OR IGNORE INTO meta VALUES(?,?)',(f'announced:{oid}','1'))
            self.s.event(oid,'notified')
        self.s.execute('DELETE FROM meta WHERE key=?',(f'provision:{oid}',))

    def card_text(self,o,client=False):
        u=self.s.user(o['user_id'])
        deadline=datetime.fromisoformat(o['deadline']).strftime('%d.%m.%Y %H:%M') if o['deadline'] else 'Не уточнён'
        text=f"<b>{self.label(o)} · {STATUSES[o['status']]}</b>\n"
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
        if not o['thread'] or self.unavailable(o):
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
        self.s.schedule(f'pin:{oid}')
        try:
            await self.bot.pin_chat_message(self.forum,msg.message_id,disable_notification=True)
            self.s.execute('DELETE FROM meta WHERE key=?',(f'pin:{oid}',))
        except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter) as e:
            self.defer_job(f'pin:{oid}',e)

    async def form(self,oid):
        o=self.s.order(oid)
        step=o['step']
        if step<0:
            return
        if step==5:
            deadline='Не указан'
            if o['deadline']:
                dt=datetime.fromisoformat(o['deadline'])
                deadline=dt.strftime('%d.%m.%Y %H:%M' if dt.hour or dt.minute else '%d.%m.%Y')
            text='<b>📋 Проверьте заявку</b>\n\n'+('\n'.join([
                'Помощь: '+escape(o['work'] or 'Не уточнено'),
                'Дисциплина: '+escape(o['subject'] or 'Не уточнена'),
                'Срок: '+deadline,'Антиплагиат: '+escape(o['anti'] or 'Не уточнено'),
                'Тема: '+escape(o['title'] or 'Не указана')]))
            msg=await self.screen(o['user_id'],text,keyboard([[('✅ Всё верно',f'approve:{oid}'),('✏️ Исправить',f'editform:{oid}')]]))
            self.s.set_order(oid,form_msg=msg.message_id); return
        if step==6:
            await self.receipt(oid); return
        values=WORK if step==0 else SUBJECT if step==1 else ANTI if step==3 else []
        rows=[[(v,f'answer:{oid}:{step}:{i}')] for i,v in enumerate(values)]
        if step==2 and not o['date_manual']:
            dates=[(f'{name} · {day:%d.%m}',f'date:{oid}:{day.isoformat()}') for name,day in quick_dates()]
            rows=[dates[i:i+2] for i in range(0,len(dates),2)]
            rows.append([('📅 Указать другую дату',f'manual:{oid}')])
        rows.append([('⬅️ Назад',f'back:{oid}:{step}'),
                     ('Не знаю, продолжить' if step==7 else 'Пропустить',f'skip:{oid}:{step}')])
        question='Если знаете требуемый процент оригинальности, напишите его сюда. Например: 70%.' if step==7 else QUESTIONS[step]
        if step==2 and not o['date_manual']: question='📅 Когда нужно подготовить работу? Выберите дату.'
        if step==3: question='🔍 Будет ли проверка на антиплагиат?'
        text=f"<b>Новая заявка · Шаг {4 if step==7 else step+1} из 5</b>\n{question}"
        if o['form_msg']:
            try:
                await self.bot.edit_message_text(text,chat_id=o['user_id'],message_id=o['form_msg'],reply_markup=keyboard(rows))
                return
            except TelegramBadRequest as e:
                if 'not modified' in str(e): return
        msg=await self.screen(o['user_id'],text,keyboard(rows))
        self.s.set_order(oid,form_msg=msg.message_id)

    async def advance(self,o,value):
        step=o['step']
        if step not in (0,1,2,3,4,7):
            return
        field='anti' if step==7 else FIELDS[step]
        if value and field=='deadline':
            value=date_value(value)
        if step==7:
            if value:
                match=re.fullmatch(r'\s*(\d{1,3}(?:[.,]\d{1,2})?)\s*%?\s*',value)
                if not match or not 0<=float(match[1].replace(',','.'))<=100:
                    raise ValueError('Укажите процент от 0 до 100, например 70%, или нажмите «Не знаю, продолжить».')
                value='Да · '+match[1].replace(',','.')+'%'
            else: value='Да · процент не уточнён'
        self.s.set_order(o['id'],**{field:value},step=7 if step==3 and value=='Да' else 4 if step==7 else step+1,date_manual=0)
        if field=='work' and value!=o['work']: self.s.schedule(f'rename:{o["id"]}',2)
        self.s.schedule(f'carddirty:{o["id"]}')
        await self.form(o['id'])

    async def receipt(self,oid):
        o=self.s.order(oid)
        if o['step'] not in (-1,6): return
        if o['receipt']:
            return
        msg=await self.bot.send_message(o['user_id'],
          '📎 Прикрепите требования, методичку и остальные материалы. Если документов нет, можно продолжить без них.',
          reply_markup=keyboard([[('✅ Материалы отправлены',f'finish:{oid}')],[('Без документов',f'nofiles:{oid}')]]))
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
            review=self.s.one('SELECT oid FROM review_sessions WHERE uid=?',(uid,))
            if review:
                if not m.text or len(m.text)>3000:
                    await self.bot.send_message(uid,'Для анонимного отзыва нужен только текст до 3000 символов. Вложения не публикуются.'); return
                if re.search(r'@|https?://|t\.me/|№\s*\d|заказ\w*\s+\d|\+?\d[\d ()-]{8,}\d',m.text,re.I):
                    await self.bot.send_message(uid,'Уберите контакты, ссылки и длинные номера из отзыва.'); return
                o=self.own(uid,review['oid'])
                existing=self.s.one('SELECT state FROM reviews WHERE oid=?',(o['id'],))
                if existing and existing['state']!='draft':
                    await self.bot.send_message(uid,'Отзыв уже передан для публикации.'); return
                self.s.execute('INSERT OR REPLACE INTO reviews(oid,body,rating,state) VALUES(?,?,?,?)',
                    (o['id'],m.text,o['rating'],'draft'))
                await self.screen(uid,'<b>Предпросмотр анонимного отзыва</b>\n'+escape(m.text)+'\n\nПроверьте, что текст не раскрывает вашу личность.',
                    keyboard([[('Опубликовать',f'publish:{o["id"]}'),('Отмена','reviewcancel')]]))
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
            if o and o['form_msg'] and m.text and o['step'] in (0,1,2,3,4,7):
                if not self.s.execute('INSERT OR IGNORE INTO form_inputs VALUES(?,?)',(m.chat.id,m.message_id)).rowcount: return
                try:
                    replied=m.reply_to_message
                    if replied and replied.message_id!=o['form_msg']:
                        raise ValueError('Это ответ на предыдущий вопрос. Ответьте на текущий экран анкеты.')
                    if o['step']==3:
                        raise ValueError('Выберите «Да», «Нет» или «Не знаю» кнопкой под вопросом.')
                    if o['step']==4 and (re.fullmatch(r'\d{1,2}[./]\d{1,2}[./]\d{2,4}',m.text.strip()) or m.text.strip().lower() in ('да','нет','не знаю','да, будет')):
                        raise ValueError('Сейчас нужна тема задания. Для изменения даты или антиплагиата нажмите «Назад».')
                    await self.advance(o,m.text[:500])
                except ValueError as e:
                    await self.bot.send_message(uid,str(e),disable_notification=True)
                return
            accepted=self.s.enqueue(m,o['id'] if o else None,'in',payload(m))
            if not accepted: return
            if not o:
                await self.choose(uid); return
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
        if cmd=='/number':
            try:
                self.s.number(o['id'],text.partition(' ')[2].strip())
                self.s.schedule(f'carddirty:{o["id"]}')
                await m.answer('Номер сохранён. Название темы и карточка обновятся автоматически с учётом ограничений Telegram.',disable_notification=True)
            except ValueError as e:
                await m.answer(str(e),disable_notification=True)
            return
        if cmd=='/retryreviews':
            self.s.execute("UPDATE reviews SET state='pending',due=0,tries=0 WHERE state='failed'")
            await m.answer('Повтор публикации отзывов запланирован.',disable_notification=True); return
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
          WHERE o.number=? OR CAST(u.id AS TEXT)=? OR u.name LIKE ?
          OR u.username LIKE ? ORDER BY o.id DESC LIMIT 20''',(query,query,term,term))
        await self.send_order_list(m.chat.id,rows,'Результаты поиска')

    async def send_order_list(self,chat,orders,title,extra=None,edit=None):
        rows=[]
        for o in orders:
            u=self.s.user(o['user_id'])
            wait=f" · ждёт {int((time.time()-o['waiting'])/60)} мин" if o['waiting'] else ''
            rows.append([InlineKeyboardButton(text=f"{self.label(o)} · {u['name'][:22]}{wait}",callback_data=f'adminorder:{o["id"]}')])
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
            await self.answer_callback(c,str(e)[:180],show_alert=True)
        except (TelegramBadRequest,TelegramForbiddenError):
            log.exception('Callback failed')
            await self.answer_callback(c,'Действие не выполнено. Откройте меню и повторите.',show_alert=True)

    async def dispatch_callback(self,c):
        data=c.data or ''
        uid=c.from_user.id
        private=c.message.chat.type=='private'
        if not private:
            if c.message.chat.id!=self.forum or uid not in self.admins:
                raise ValueError('Доступ только менеджеру')
            await self.answer_callback(c)
            await self.admin_callback(c,data); return
        self.s.upsert_user(c.from_user)
        if self.s.user(uid)['blocked']:
            raise ValueError('Доступ ограничен. Свяжитесь с OneStudy другим способом.')
        await self.answer_callback(c)
        mid=getattr(c.message,'message_id',None)
        screen=self.s.one('SELECT mid FROM screens WHERE uid=?',(uid,))
        if isinstance(mid,int) and screen and data.split(':')[0] in ('new','confirmnew','talk','mine','view','select','faq','publish','reviewcancel') and screen['mid']!=mid:
            raise ValueError('Это старое меню. Используйте последнее или нажмите «🏠 Главное меню».')
        if data=='new':
            await self.consume(c,'new')
            old=self.s.order(self.s.user(uid)['selected'])
            if old and old['status'] not in TERMINAL:
                if old['step']>=0 or (old['status']=='new' and not old['work'] and not old['title'] and not old['number']):
                    if old['step']<0: self.s.set_order(old['id'],step=0)
                    await self.clear_screen(uid)
                    await self.form(old['id']); return
                await self.screen(uid,'У вас уже есть активная заявка. Продолжить её или создать отдельную?',
                    keyboard([[('Продолжить',f'select:{old["id"]}'),('Создать отдельную','confirmnew')]])); return
            o=await self.create(uid,True)
            await self.clear_screen(uid)
            await self.form(o['id'])
        elif data=='confirmnew':
            await self.consume(c,'confirmnew')
            o=await self.create(uid,True)
            await self.clear_screen(uid); await self.form(o['id'])
        elif data=='talk':
            orders=self.s.orders(uid)
            if len(orders)>1 or (orders and orders[0]['status'] in TERMINAL):
                await self.choose(uid)
            else:
                await self.screen(uid,'Напишите вопрос или отправьте материалы сюда.')
        elif data.startswith('mine:'):
            page=max(0,int(data.split(':')[1]))
            orders=self.s.orders(uid)[page*8:page*8+9]
            rows=[[(f"{self.label(o)} · {STATUSES[o['status']]}",f'view:{o["id"]}')] for o in orders[:8]]
            if page: rows.append([('⬅️ Назад',f'mine:{page-1}')])
            if len(orders)>8: rows.append([('Далее ➡️',f'mine:{page+1}')])
            await self.screen(uid,'📂 Мои заказы' if orders else 'Пока заказов нет.',keyboard(rows))
        elif data.startswith('view:'):
            o=self.own(uid,int(data.split(':')[1]))
            await self.screen(uid,self.card_text(o,True),keyboard([[('💬 Написать / добавить материалы',f'select:{o["id"]}')]]))
        elif data.startswith('select:'):
            oid=int(data.split(':')[1]); self.own(uid,oid)
            await self.consume(c,'select')
            await self.clear_screen(uid)
            self.s.select(uid,oid)
            await self.remove(uid,getattr(c.message,'message_id',None))
            await self.screen(uid,f'Выбрана: {self.label(self.s.order(oid))}. Напишите сообщение или отправьте материалы.')
            await self.receipt(oid)
        elif data.startswith(('answer:','skip:','back:')):
            parts=data.split(':'); oid,step=int(parts[1]),int(parts[2]); o=self.current(uid,oid)
            if o['step']!=step or o['status'] in TERMINAL:
                raise ValueError('Этот шаг уже изменился. Откройте актуальную анкету.')
            if isinstance(mid,int) and o['form_msg']!=mid:
                raise ValueError('Эта анкета больше не активна. Откройте её из главного меню.')
            if parts[0]=='back':
                previous=3 if step==7 else (7 if step==4 and o['anti'].startswith('Да') else 3) if step==4 else max(0,step-1)
                self.s.set_order(oid,step=previous,date_manual=0); await self.form(oid)
            else:
                options=WORK if step==0 else SUBJECT if step==1 else ANTI if step==3 else []
                if parts[0]=='answer' and (len(parts)!=4 or not parts[3].isdigit() or int(parts[3])>=len(options)):
                    raise ValueError('Используйте актуальные кнопки.')
                value=options[int(parts[3])] if parts[0]=='answer' else ''
                await self.advance(o,value)
        elif data.startswith(('date:','manual:')):
            parts=data.split(':'); oid=int(parts[1]); o=self.current(uid,oid)
            if o['step']!=2 or (isinstance(mid,int) and o['form_msg']!=mid):
                raise ValueError('Выбор даты уже завершён. Используйте текущий экран.')
            if parts[0]=='manual':
                self.s.set_order(oid,date_manual=1); await self.form(oid)
            else:
                try: value=datetime.strptime(parts[2],'%Y-%m-%d').strftime('%d.%m.%Y')
                except ValueError: raise ValueError('Дата недоступна. Откройте выбор даты заново.') from None
                await self.advance(o,value)
        elif data.startswith(('approve:','editform:')):
            oid=int(data.split(':')[1]); o=self.current(uid,oid)
            if o['step']!=5 or (isinstance(mid,int) and o['form_msg']!=mid):
                raise ValueError('Это устаревшая сводка. Используйте текущий экран.')
            await self.consume(c,'summary')
            await self.clear_screen(uid)
            await self.remove(uid,o['receipt'])
            self.s.set_order(oid,receipt=None,finish=0,step=0 if data.startswith('editform:') else 6)
            if data.startswith('editform:'): await self.form(oid)
            else:
                self.s.set_order(oid,waiting=o['waiting'] or time.time(),activity=time.time())
                self.s.schedule(f'carddirty:{oid}')
                await self.receipt(oid)
        elif data.startswith(('finish:','nofiles:')):
            oid=int(data.split(':')[1]); o=self.current(uid,oid)
            if o['status'] in TERMINAL: raise ValueError('Заказ уже завершён')
            if o['step'] not in (-1,6): raise ValueError('Сначала заполните и подтвердите анкету.')
            if isinstance(mid,int) and o['receipt'] and mid!=o['receipt']:
                raise ValueError('Это старая кнопка. Используйте актуальное сообщение о материалах.')
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
            oid=int(data.split(':')[1]); o=self.own(uid,oid)
            if not self.reviews_chat: raise ValueError('Группа отзывов пока не настроена.')
            existing=self.s.one('SELECT state FROM reviews WHERE oid=?',(oid,))
            if existing and existing['state']!='draft': raise ValueError('Отзыв уже сохранён для публикации.')
            if o['rating'] is None: raise ValueError('Сначала поставьте оценку.')
            await self.consume(c,'review')
            self.s.execute('INSERT OR REPLACE INTO review_sessions VALUES(?,?)',(uid,oid))
            await self.remove(uid,getattr(c.message,'message_id',None))
            await self.screen(uid,'Отзыв будет опубликован анонимно. Не указывайте имя, контакты, номер заказа и сведения, по которым вас можно узнать.\nОтправьте только текст.',keyboard([[('Отмена','reviewcancel')]]))
        elif data=='reviewcancel':
            session=self.s.one('SELECT oid FROM review_sessions WHERE uid=?',(uid,))
            if session: self.s.execute("DELETE FROM reviews WHERE oid=? AND state='draft'",(session['oid'],))
            self.s.execute('DELETE FROM review_sessions WHERE uid=?',(uid,))
            await self.screen(uid,'Публикация отменена.')
        elif data.startswith('publish:'):
            oid=int(data.split(':')[1]); self.own(uid,oid)
            session=self.s.one('SELECT oid FROM review_sessions WHERE uid=?',(uid,))
            if not session or session['oid']!=oid: raise ValueError('Начните отзыв заново.')
            await self.consume(c,'publish')
            self.s.execute("UPDATE reviews SET state='pending' WHERE oid=? AND state='draft'",(oid,))
            self.s.execute('DELETE FROM review_sessions WHERE uid=?',(uid,))
            await self.screen(uid,'Спасибо! Отзыв сохранён и поставлен в очередь анонимной публикации.')
        elif data=='faq':
            await self.screen(uid,'Частые вопросы',keyboard([[(v[0],'faq:'+k)] for k,v in FAQ.items()]))
        elif data.startswith('faq:'):
            key=data.split(':')[1]
            if key not in FAQ: raise ValueError('Вопрос не найден')
            await self.screen(uid,FAQ[key][1],keyboard([[('⬅️ К вопросам','faq')]]))

    async def admin_callback(self,c,data):
        if data.startswith('dash:'):
            _,kind,page=data.split(':'); await self.dashboard(self.forum,kind,max(0,int(page)),c.message); return
        parts=data.split(':')
        if len(parts)<2 or not parts[1].isdigit(): return
        oid=int(parts[1]); o=self.s.order(oid)
        if not o: raise ValueError('Заказ не найден')
        if parts[0]=='adminorder':
            text=self.card_text(o)
            markup=keyboard([[('📁 Закрыть заявку',f'archiveask:{oid}')],[('⬅️ Назад к списку','dash:all:0')]])
            if o['thread'] and not self.unavailable(o):
                url=f"https://t.me/c/{str(self.forum).removeprefix('-100')}/{o['thread']}"
                markup.inline_keyboard.insert(0,[InlineKeyboardButton(text='💬 Перейти в переписку',url=url)])
            else: text+='\n\n⚠️ Переписка недоступна или ещё не создана. Закрытие в архив доступно.'
            await c.message.edit_text(text,reply_markup=markup)
        elif parts[0]=='archiveask':
            count=self.s.one("SELECT COUNT(*) n FROM queue WHERE oid=? AND state!='sent'",(oid,))['n']
            text=f'Закрыть в архив: {self.label(o)}?\nКлиенту не придёт сообщение или запрос оценки. Напоминания прекратятся.'
            if count: text+=f'\nЕсть недоставленные сообщения: {count}. Они сохранятся в базе, но их передача будет приостановлена.'
            await c.message.edit_text(text,reply_markup=keyboard([[('📁 Да, закрыть',f'archiveconfirm:{oid}')],[('Отмена',f'adminorder:{oid}')]]))
        elif parts[0]=='archiveconfirm':
            if o['status']=='archive':
                await c.message.edit_text('📁 Заявка уже в архиве.',reply_markup=keyboard([[('⬅️ К активным заявкам','dash:all:0')]]))
                return
            # Commit locally; a missing/deleted topic must not prevent closing.
            with self.s.c:
                self.s.c.execute("UPDATE orders SET status='archive',waiting=NULL,finish=0,step=-1 WHERE id=?",(oid,))
                self.s.c.execute('DELETE FROM meta WHERE key IN (?,?,?,?)',
                    (f'rename:{oid}',f'carddirty:{oid}',f'close:{oid}',f'failed_close:{oid}'))
                self.s.c.execute('INSERT INTO events(oid,action,created) VALUES(?,?,?)',
                    (oid,'Закрыто в архив из сводки без уведомления клиента',time.time()))
            await c.message.edit_text('📁 Заявка закрыта в архив. Данные сохранены, клиент не уведомлён.',
                reply_markup=keyboard([[('⬅️ К активным заявкам','dash:all:0')]]))
            if o['thread'] and not self.unavailable(o):
                try: await self.card(oid)
                except TelegramBadRequest as e:
                    if self.is_missing_topic(e): await self.missing_topic(o)
                    else: log.warning('Archived card update deferred')
                except (TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter):
                    log.warning('Archived card update deferred')
        elif parts[0]=='status':
            await c.message.reply('Выберите статус:',reply_markup=keyboard([[(v,f'set:{oid}:{k}')] for k,v in STATUSES.items() if k!='done']),disable_notification=True)
        elif parts[0]=='set':
            status=parts[2]
            if status not in STATUSES or status=='done': raise ValueError('Недопустимый статус')
            self.s.set_order(oid,status=status,activity=time.time(),warned=0)
            self.s.event(oid,'Статус: '+status)
            await self.card(oid)
            await self.remove(self.forum,getattr(c.message,'message_id',None))
        elif parts[0]=='card':
            await self.card(oid)
            history=self.s.all('SELECT action,created FROM events WHERE oid=? AND action!=? ORDER BY id DESC LIMIT 8',(oid,'notified'))
            details=self.card_text(self.s.order(oid))+'\n\n<b>История</b>\n'+'\n'.join(escape(r['action'][:200]) for r in history)
            await c.message.reply(details,disable_notification=True)
        elif parts[0]=='close':
            await c.message.reply(f'Завершить {self.label(o)}?',reply_markup=keyboard([[('Завершить',f'confirm:{oid}'),('Отмена',f'cancelclose:{oid}')]]),disable_notification=True)
        elif parts[0]=='cancelclose':
            await self.remove(self.forum,getattr(c.message,'message_id',None))
        elif parts[0]=='confirm':
            if o['status']=='done': return
            if o['status'] in ('archive','cancelled'):
                raise ValueError('Заявка закрыта. Старую кнопку завершения использовать нельзя.')
            if self.s.one("SELECT id FROM queue WHERE oid=? AND state!='sent' LIMIT 1",(oid,)):
                raise ValueError('Сначала дождитесь передачи сообщений или исправьте ошибку через /retry.')
            # Commit status and outgoing notice together, before the Telegram call.
            with self.s.c:
                self.s.c.execute("UPDATE orders SET status='done',step=-1,waiting=NULL,finish=0 WHERE id=?",(oid,))
                self.s.c.execute('INSERT INTO events(oid,action,created) VALUES(?,?,?)',(oid,'Завершён менеджером',time.time()))
                self.s.c.execute('INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)',
                    (f'close:{oid}',json.dumps({'uid':o['user_id'],'due':0,'tries':0})))
            await self.remove(self.forum,getattr(c.message,'message_id',None))
            await self.card(oid)

    async def rename(self,oid):
        o=self.s.order(oid)
        if self.unavailable(o):
            self.s.execute('DELETE FROM meta WHERE key=?',(f'rename:{oid}',))
            return
        if not o['thread']: return
        name=self.topic_name(o)
        saved=json.dumps([self.forum,o['thread'],name])
        cached=self.s.one('SELECT value FROM meta WHERE key=?',(f'topicname:{oid}',))
        if cached and cached['value']==saved:
            self.s.execute('DELETE FROM meta WHERE key=?',(f'rename:{oid}',)); return
        try: await self.bot.edit_forum_topic(self.forum,o['thread'],name=name)
        except TelegramBadRequest as e:
            if 'not modified' not in str(e): raise
        self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(f'topicname:{oid}',saved))
        if self.topic_name(self.s.order(oid))==name:
            self.s.execute('DELETE FROM meta WHERE key=?',(f'rename:{oid}',))

    async def publish_reviews(self):
        if not self.reviews_chat: return
        for r in self.s.all("SELECT * FROM reviews WHERE state='pending' AND due<=? LIMIT 3",(time.time(),)):
            grade=f"Оценка: {r['rating']}/5\n" if r['rating'] else ''
            try:
                # Never forward/copy, never include identifiers, metadata or links.
                await self.bot.send_message(self.reviews_chat,'💬 <b>Новый отзыв</b>\n'+grade+'\n'+escape(r['body']),
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
                self.s.execute("UPDATE reviews SET state='sent' WHERE oid=?",(r['oid'],))
            except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter) as e:
                tries=r['tries']+(0 if isinstance(e,TelegramRetryAfter) else 1)
                delay=e.retry_after+1 if isinstance(e,TelegramRetryAfter) else min(300,2**tries)
                self.s.execute('UPDATE reviews SET state=?,tries=?,due=? WHERE oid=?',
                    ('failed' if tries>=8 and not isinstance(e,TelegramRetryAfter) else 'pending',tries,time.time()+delay,r['oid']))
                if tries==1 and not isinstance(e,TelegramRetryAfter): await self.alert('⚠️ Не удалось опубликовать анонимный отзыв. Проверьте REVIEWS_CHAT_ID и права бота.')

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
        except TelegramRetryAfter:
            log.info('Admin alert deferred by Telegram')
        except Exception:
            log.error('Unable to deliver admin alert')

    async def delivery_issue(self,row,error):
        o=self.s.order(row['oid'])
        if row['direction']=='out':
            if not row['alerted']:
                await self.alert(f'⚠️ Ответ по {self.label(o)} пока не доставлен клиенту. Причина: {error}. Проверьте доступ и /retry.',o['id'])
                self.s.execute('UPDATE queue SET alerted=1 WHERE id=?',(row['id'],))
            return
        if not o['delay_admin']:
            try:
                msg=await self.bot.send_message(self.owner or self.forum,
                    f'⚠️ Не удаётся передать сообщения: {self.label(o)}. Причина: {error}. Проверьте права и /retry.',
                    message_thread_id=o['thread'] if not self.owner else None)
                self.s.set_order(o['id'],delay_admin=msg.message_id)
            except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter):
                return
        if not o['delay_msg']:
            try:
                msg=await self.bot.send_message(o['user_id'],
                  'Передача сообщений временно задерживается. Они сохранены; менеджеру отправлено уведомление.',disable_notification=True)
                self.s.set_order(o['id'],delay_msg=msg.message_id)
            except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter):
                pass

    async def deliver(self,rows):
        r=rows[0]; o=self.s.order(r['oid'])
        if self.unavailable(o): raise ValueError('Рабочая тема недоступна; данные сохранены')
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
        # Card UI errors are not message delivery errors.
        self.s.schedule(f'carddirty:{o["id"]}')

    async def tick(self):
        await self.publish_reviews()
        for item in self.ready_jobs('rename'):
            try: await self.rename(int(item['key'].split(':')[1]))
            except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter) as e:
                if self.is_missing_topic(e): await self.missing_topic(self.s.order(int(item['key'].split(':')[1])))
                else: self.defer_job(item['key'],e)
        for o in self.s.all("SELECT * FROM orders WHERE (thread IS NULL OR EXISTS(SELECT 1 FROM meta m WHERE m.key='provision:' || orders.id)) AND status NOT IN ('archive','cancelled','done') LIMIT 5"):
            try: await self.ensure_topic(o['id'])
            except TelegramRetryAfter: pass
            except Exception:
                log.exception('Topic creation delayed for order %s',o['id'])
        for item in self.ready_jobs('carddirty'):
            oid=int(item['key'].split(':')[1])
            try:
                await self.card(oid)
                if self.s.order(oid)['thread']: self.s.execute('DELETE FROM meta WHERE key=? AND value=?',(item['key'],item['value']))
            except (TelegramBadRequest,TelegramForbiddenError,TelegramRetryAfter,TelegramNetworkError) as e:
                if self.is_missing_topic(e): await self.missing_topic(self.s.order(oid))
                else: self.defer_job(item['key'],e)
        for item in self.ready_jobs('pin'):
            o=self.s.order(int(item['key'].split(':')[1]))
            if not o or not o['card'] or self.unavailable(o) or o['status'] in TERMINAL:
                self.s.execute('DELETE FROM meta WHERE key=?',(item['key'],)); continue
            try:
                await self.bot.pin_chat_message(self.forum,o['card'],disable_notification=True)
                self.s.execute('DELETE FROM meta WHERE key=?',(item['key'],))
            except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter) as e:
                self.defer_job(item['key'],e)
        rows=self.s.next_batch()
        if rows:
            try: await self.deliver(rows)
            except Exception as e:
                if self.is_missing_topic(e): await self.missing_topic(self.s.order(rows[0]['oid']))
                permanent=isinstance(e,(TelegramForbiddenError,ValueError))
                limited=isinstance(e,TelegramRetryAfter)
                tries=rows[0]['tries']+(0 if limited else 1)
                failed=not limited and (permanent or tries>=8)
                delay=e.retry_after+1 if isinstance(e,TelegramRetryAfter) else min(300,2**tries)
                for row in rows:
                    # A card refresh failure after delivery must never resend the content.
                    if self.s.one('SELECT state FROM queue WHERE id=?',(row['id'],))['state']=='sent': continue
                    self.s.execute('UPDATE queue SET state=?,tries=?,due=?,error=? WHERE id=?',
                        ('failed' if failed else 'pending',tries,time.time()+delay,type(e).__name__,row['id']))
                unsent=any(self.s.one('SELECT state FROM queue WHERE id=?',(r['id'],))['state']!='sent' for r in rows)
                # One quick retry/rate limit is normal, not a client-facing incident.
                if unsent and not isinstance(e,TelegramRetryAfter) and (failed or (tries>=3 and time.time()-rows[0]['created']>=30)):
                    await self.delivery_issue(rows[0],type(e).__name__)
        for o in self.s.all('SELECT * FROM orders WHERE delay_msg IS NOT NULL'):
            if not self.s.pending(o['id']):
                try:
                    await self.remove(o['user_id'],o['delay_msg'])
                    self.s.set_order(o['id'],delay_msg=None)
                    if o['delay_admin']:
                        await self.remove(self.owner or self.forum,o['delay_admin'])
                        self.s.set_order(o['id'],delay_admin=None)
                except TelegramRetryAfter: pass
                except (TelegramForbiddenError,TelegramNetworkError):
                    log.warning('Recovery notice update deferred')
        for o in self.s.all('SELECT * FROM orders WHERE finish=1'):
            if self.unavailable(o) or o['status'] in ('archive','cancelled'): continue
            if self.s.user(o['user_id'])['selected']!=o['id']: continue
            if self.s.pending(o['id']): continue
            if not o['thread'] or not o['card'] or self.s.one('SELECT value FROM meta WHERE key=?',(f'carddirty:{o["id"]}',)):
                continue
            try:
                await self.bot.edit_message_text(f"✅ <b>Заявка передана менеджеру</b>\n\nОтвет придёт в этот чат. Дополнить заявку можно обычным сообщением.\n\n💡 <b>Учебная минутка</b>\n{escape(o['fact'] or '')}",
                    chat_id=o['user_id'],message_id=o['receipt'])
            except TelegramBadRequest as e:
                if 'not modified' not in str(e):
                    await self.alert(f"Не удалось обновить подтверждение заказа {self.label(o)}",o['id'])
            except TelegramForbiddenError:
                await self.alert(f"Клиент заказа {self.label(o)} недоступен для подтверждения.",o['id'])
            except TelegramRetryAfter: continue
            self.s.set_order(o['id'],finish=0,batch=0,step=-1)
        for item in self.s.all("SELECT * FROM meta WHERE key LIKE 'close:%' LIMIT 5"):
            oid=int(item['key'].split(':')[1]); o=self.s.order(oid)
            notice=json.loads(item['value'])
            if notice['due']>time.time(): continue
            if o['status']!='done':
                self.s.execute('DELETE FROM meta WHERE key=?',(item['key'],)); continue
            try:
                await self.bot.send_message(notice['uid'],f'{self.label(o)} завершён. Спасибо за обращение!\nОцените, пожалуйста, как прошла работа.',
                    reply_markup=keyboard([[(f'{n}/5',f'rate:{oid}:{n}') for n in range(1,6)]]))
            except TelegramForbiddenError:
                await self.alert(f'Клиент {self.label(o)} заблокировал бота. Уведомление о завершении не доставлено.',oid)
                self.s.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(f'failed_close:{oid}',item['value']))
            except TelegramBadRequest:
                await self.alert(f'Не удалось отправить завершение {self.label(o)}. После устранения причины используйте /retry.',oid)
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
            if self.unavailable(o): continue
            try:
                await self.maintain_order(o,now)
            except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter) as e:
                if self.is_missing_topic(e): await self.missing_topic(o)
                elif not isinstance(e,TelegramRetryAfter): log.warning('Maintenance deferred for internal order %s: %s',o['id'],type(e).__name__)

    async def maintain_order(self,o,now):
        if o['thread'] and o['waiting'] and now-o['waiting']>=1800 and not o['reminded']:
            await self.bot.send_message(self.forum,f"⏰ Клиент по заказу {self.label(o)} ждёт ответа.",message_thread_id=o['thread'])
            self.s.set_order(o['id'],reminded=1)
        days=(now-o['activity'])/86400
        if days>=30 and o['status'] in ('new','clarify') and not self.s.pending(o['id']):
            self.s.set_order(o['id'],status='archive'); self.s.event(o['id'],'Архив по неактивности'); await self.card(o['id'])
        elif days>=7 and not o['warned'] and o['thread']:
            await self.bot.send_message(self.forum,'⚠️ Нет переписки 7 дней. Проверьте состояние заказа.',message_thread_id=o['thread'],disable_notification=True)
            self.s.set_order(o['id'],warned=1)

    async def guide(self):
        if self.s.one("SELECT value FROM meta WHERE key='guide_v33'"): return
        thread=self.s.one("SELECT value FROM meta WHERE key='guide_thread_v34'")
        if not thread:
            t=await self.bot.create_forum_topic(self.forum,name='📖 OneStudy · инструкция v3.4')
            self.s.execute("INSERT INTO meta VALUES('guide_thread_v34',?)",(str(t.message_thread_id),))
            thread={'value':str(t.message_thread_id)}
        tid=int(thread['value'])
        saved=self.s.one("SELECT value FROM meta WHERE key='guide_msg_v34'")
        if not saved:
            msg=await self.bot.send_message(self.forum,
          '<b>OneStudy v3.4 · менеджеру</b>\n\n'
          'В /orders нажмите заявку: откроются переход в переписку и закрытие в архив. '
          'Архив из списка не уведомляет клиента и работает даже с удалённой темой.\n\n'
          'Анкета: дата кнопкой или вручную → антиплагиат Да/Нет/Не знаю → '
          'необязательный процент при Да → тема → сводка → материалы.\n'
          'Ответы анкеты сохраняются в карточке, без отдельной пересылки каждого поля.\n\n'
          '/orders или /clients — список заказов.\n'
          '/search Анна — поиск по имени; также можно указать номер или Telegram ID.\n'
          '/done — завершение с подтверждением в теме заказа.\n'
          '/number 125 — ваш номер заказа: меняет тему, карточку и сводки.\n'
          '/retryreviews — повторить неудачные публикации анонимных отзывов (из темы заказа).\n'
          '/note текст — внутренняя заметка, клиент её не получит.\n'
          '/retry — повторить неудачные отправки в этой теме.\n'
          '/block и /unblock — ограничить или восстановить приём сообщений клиента.\n\n'
          '<b>Обычные сообщения в клиентской теме уходят клиенту.</b>\n'
          'Статусы меняются кнопкой карточки. Новые заказы не заменяют старые.\n'
          'Напоминание через 30 минут действует круглосуточно: график пока не настроен.\n'
          'Рабочие заказы автоматически не закрываются.\n'
          'При ошибке доставки проверьте права и доступность клиента, затем используйте /retry.\n'
          'При частично переданном альбоме сначала проверьте тему: повтор может создать дубли.',
          message_thread_id=tid,disable_notification=True,
          reply_markup=keyboard([[('📋 Заказы','dash:all:0')]]))
            self.s.execute("INSERT INTO meta VALUES('guide_msg_v34',?)",(str(msg.message_id),))
            saved={'value':str(msg.message_id)}
        await self.bot.pin_chat_message(self.forum,int(saved['value']),disable_notification=True)
        self.s.execute("INSERT OR REPLACE INTO meta VALUES('guide_v33',?)",(str(tid),))

    async def worker(self):
        BACKGROUND_REQUEST.set(True)
        last_maintenance=0; last_backup=0
        while True:
            try:
                await self.tick()
                now=time.time()
                if now-last_maintenance>60:
                    last_maintenance=now
                    await self.maintenance()
                if now-last_backup>86400:
                    path=self.s.backup(self.s.path.parent/'backups')
                    if self.owner:
                        await self.bot.send_document(self.owner,FSInputFile(path),caption='Резервная копия OneStudy. Содержит данные клиентов.',disable_notification=True)
                    last_backup=now
            except asyncio.CancelledError: raise
            except TelegramRetryAfter:
                log.info('Worker request deferred by Telegram')
            except Exception:
                log.exception('Worker cycle failed')
            await asyncio.sleep(1.2)


async def main():
    logging.basicConfig(level=logging.INFO)
    log.info('Starting OneStudy v3.4')
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
    rate_guard=RateGuard(store)
    bot.session.middleware(rate_guard)
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
        reviews_chat=int(os.environ.get('REVIEWS_CHAT_ID','0')) or None
        app=App(store,bot,forum,admins,owner,reviews_chat,rate_guard)
        context_token=BACKGROUND_REQUEST.set(True)
        try: await app.guide()
        except (TelegramBadRequest,TelegramForbiddenError,TelegramNetworkError,TelegramRetryAfter):
            log.warning('Не удалось создать служебную инструкцию; см. README.md')
        finally: BACKGROUND_REQUEST.reset(context_token)
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
