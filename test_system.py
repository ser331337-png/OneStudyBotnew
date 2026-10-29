import json
import sqlite3
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import pytest
from aiogram.types import Message, User
from aiogram.exceptions import TelegramNetworkError, TelegramForbiddenError, TelegramBadRequest
from aiogram.methods import CopyMessage
from bot import App, date_value, payload
from db import Store
from facts import FACTS


@pytest.fixture
def s(tmp_path):
    x=Store(tmp_path/'threads.db')
    x.upsert_user(User(id=1,is_bot=False,first_name='Анна <&>'))
    x.upsert_user(User(id=2,is_bot=False,first_name='Другой'))
    yield x
    x.c.close()


def message(mid=1,uid=1,**kw):
    return Message.model_validate(dict(message_id=mid,date=int(time.time()),
      chat={'id':uid,'type':'private'},from_user={'id':uid,'is_bot':False,'first_name':'Анна'},**kw))


@pytest.fixture
def app(s):
    bot=NS(**{name:AsyncMock() for name in ('send_message','send_photo','edit_message_text',
      'edit_message_caption','pin_chat_message','copy_message','copy_messages','send_media_group','get_user_profile_photos','create_forum_topic')})
    bot.send_message.return_value=NS(message_id=90)
    bot.send_photo.return_value=NS(message_id=91)
    bot.create_forum_topic.return_value=NS(message_thread_id=100)
    bot.get_user_profile_photos.return_value=NS(photos=[])
    return App(s,bot,-100123,{99})


def cb(data,uid=1,chat=1):
    return NS(data=data,from_user=User(id=uid,is_bot=False,first_name='Анна'),
      answer=AsyncMock(),message=NS(chat=NS(id=chat,type='private' if chat>0 else 'supergroup'),
      edit_text=AsyncMock(),reply=AsyncMock()))


def test_multiple_orders_history(s):
    a=s.new_order(1); s.set_order(a['id'],status='done',rating=5,thread=100)
    b=s.new_order(1)
    assert len(s.orders(1))==2
    assert s.order(a['id'])['rating']==5
    assert s.order(b['id'])['status']=='new'


def test_migration(tmp_path):
    path=tmp_path/'old.db'
    with sqlite3.connect(path) as c:
        c.execute('CREATE TABLE threads(user_id INTEGER PRIMARY KEY,thread_id INTEGER, status TEXT,topic_name TEXT,rating INTEGER)')
        c.execute("INSERT INTO threads VALUES(1,777,'done','Заказ',4)")
    s=Store(path)
    assert s.orders(1)[0]['thread']==777
    assert s.orders(1)[0]['rating']==4
    s.c.close(); s=Store(path)
    assert len(s.orders(1))==1
    s.c.close()


def test_duplicates_and_many_files(s):
    o=s.new_order(1)
    for n in range(20):
        m=message(n+1,document={'file_id':f'f{n}','file_unique_id':f'u{n}'})
        assert s.enqueue(m,o['id'],'in',payload(m))
        assert not s.enqueue(m,o['id'],'in',payload(m))
    assert s.pending(o['id'])==20


def test_pending_survives_restart(s):
    s.enqueue(message(text='Первое'),None,'in',{'text':'Первое'})
    s.enqueue(message(2,text='Второе'),None,'in',{'text':'Второе'})
    other=Store(s.path)
    o=other.new_order(1)
    assert other.pending(o['id'])==2
    assert other.order(o['id'])['waiting']
    other.c.close()


def test_recover_sending_and_no_resend_delivered(s):
    o=s.new_order(1)
    s.enqueue(message(text='x'),o['id'],'in',{'text':'x'})
    s.execute("UPDATE queue SET state='sending'")
    other=Store(s.path)
    assert other.next_batch()[0]['state']=='pending'
    other.execute("UPDATE queue SET state='sent'")
    assert other.next_batch()==[]
    other.c.close()


def test_facts_no_repeat(s):
    assert len(FACTS)==45 and len(set(FACTS))==45
    assert len({s.fact(1,FACTS) for _ in range(45)})==45
    assert s.fact(1,FACTS) in FACTS


def test_backup(s,tmp_path):
    s.new_order(1)
    path=s.backup(tmp_path/'copies')
    with sqlite3.connect(path) as c:
        assert c.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        assert c.execute('SELECT COUNT(*) FROM orders').fetchone()[0]==1


def test_date_validation():
    assert date_value('31.12.2099').startswith('2099-12-31')
    for text in ('31.02.2099','01.01.2000','завтра','99.99.2099'):
        with pytest.raises(ValueError): date_value(text)


async def test_avatar_and_escaped_caption(app,s):
    o=s.new_order(1)
    app.bot.get_user_profile_photos.return_value=NS(photos=[[NS(file_id='avatar')]])
    await app.ensure_topic(o['id'])
    assert s.order(o['id'])['photo']==1
    assert '&lt;&amp;&gt;' in app.bot.send_photo.call_args.kwargs['caption']
    await app.card(o['id'])
    assert app.bot.send_photo.await_count==1
    assert app.bot.edit_message_caption.await_count==1


async def test_no_avatar(app,s):
    o=s.new_order(1); await app.ensure_topic(o['id'])
    assert s.order(o['id'])['photo']==0
    assert s.order(o['id'])['card']==90


async def test_first_notification_then_silent(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    s.enqueue(message(text='Первое'),o['id'],'in',{'text':'Первое'})
    await app.deliver(s.next_batch())
    assert app.bot.copy_message.call_args.kwargs['disable_notification'] is False
    s.enqueue(message(2,text='Ещё'),o['id'],'in',{'text':'Ещё'})
    await app.deliver(s.next_batch())
    assert app.bot.copy_message.call_args.kwargs['disable_notification'] is True


async def test_network_retry_does_not_drop(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    s.enqueue(message(text='x'),o['id'],'in',{'text':'x'})
    app.bot.copy_message.side_effect=TelegramNetworkError(method=CopyMessage(chat_id=1,from_chat_id=1,message_id=1),message='offline')
    await app.tick()
    row=s.one('SELECT * FROM queue')
    assert row['state']=='pending' and row['tries']==1 and s.order(o['id'])['waiting']
    app.bot.copy_message.side_effect=None
    s.execute('UPDATE queue SET due=0')
    await app.tick()
    assert s.one('SELECT state FROM queue')['state']=='sent'


async def test_failed_client_delivery_keeps_waiting(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90,waiting=time.time())
    m=message(uid=99,text='Ответ')
    s.enqueue(m,o['id'],'out',{'text':'Ответ'})
    app.bot.copy_message.side_effect=TelegramForbiddenError(method=CopyMessage(chat_id=1,from_chat_id=1,message_id=1),message='blocked')
    await app.tick()
    assert s.one('SELECT state FROM queue')['state']=='failed'
    assert s.order(o['id'])['waiting']


async def test_album(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    for i in range(1,4):
        m=message(i,media_group_id='album',photo=[{'file_id':f'f{i}','file_unique_id':f'u{i}','width':100,'height':100}])
        s.enqueue(m,o['id'],'in',payload(m))
    s.execute('UPDATE queue SET due=0')
    app.bot.send_media_group.return_value=[NS(message_id=i) for i in range(3)]
    await app.tick()
    assert [m.media for m in app.bot.send_media_group.call_args.kwargs['media']]==['f1','f2','f3']
    assert s.pending(o['id'])==0


async def test_partial_album_retained_for_manual_check(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    for i in (1,2):
        m=message(i,text='x',media_group_id='album'); s.enqueue(m,o['id'],'in',{'text':'x'})
    s.execute('UPDATE queue SET due=0')
    app.bot.copy_messages.return_value=[NS(message_id=4)]
    await app.tick()
    assert s.pending(o['id'])==2
    assert s.one('SELECT state FROM queue')['state']=='failed'


async def test_one_receipt_many_messages(app,s):
    s.new_order(1)
    for i in range(1,21): await app.private(message(i,text='материал'))
    assert app.bot.send_message.await_count==1
    assert s.one('SELECT COUNT(*) n FROM queue')['n']==20


async def test_ownership_and_rating(app,s):
    o=s.new_order(1); s.set_order(o['id'],status='done',thread=100,card=90)
    with pytest.raises(ValueError): await app.dispatch_callback(cb(f'rate:{o["id"]}:5',uid=2,chat=2))
    with pytest.raises(ValueError): await app.dispatch_callback(cb(f'rate:{o["id"]}:9'))
    await app.dispatch_callback(cb(f'rate:{o["id"]}:5'))
    assert s.order(o['id'])['rating']==5
    with pytest.raises(ValueError): await app.dispatch_callback(cb(f'rate:{o["id"]}:4'))


async def test_notes_commands_and_unauthorized(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100)
    for uid,text in ((99,'/note внутреннее'),(99,'/unknown'),(2,'Чужой ответ')):
        m=message(uid=uid,text=text).model_copy(update={'message_thread_id':100})
        await app.group(m)
    assert s.one('SELECT COUNT(*) n FROM queue')['n']==0
    with pytest.raises(ValueError):
        await app.dispatch_callback(cb(f'close:{o["id"]}',uid=2,chat=-100123))


async def test_close_confirm_once(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    c=cb(f'confirm:{o["id"]}',uid=99,chat=-100123)
    await app.dispatch_callback(c); await app.dispatch_callback(c)
    assert s.order(o['id'])['status']=='done'
    await app.tick(); await app.tick()
    messages=[x for x in app.bot.send_message.call_args_list if 'завершён' in str(x)]
    assert len(messages)==1


async def test_no_autoclose_paid_work(app,s):
    a=s.new_order(1); b=s.new_order(2)
    s.set_order(a['id'],status='working',activity=time.time()-40*86400)
    s.set_order(b['id'],status='clarify',activity=time.time()-40*86400)
    await app.maintenance()
    assert s.order(a['id'])['status']=='working'
    assert s.order(b['id'])['status']=='archive'


async def test_wait_clock_not_reset_by_additions(app,s):
    o=s.new_order(1)
    s.enqueue(message(text='x'),o['id'],'in',{'text':'x'})
    old=s.order(o['id'])['waiting']
    s.enqueue(message(2,text='ещё'),o['id'],'in',{'text':'ещё'})
    assert s.order(o['id'])['waiting']==old


async def test_stale_form_callback(app,s):
    o=s.new_order(1); s.set_order(o['id'],step=2)
    with pytest.raises(ValueError): await app.dispatch_callback(cb(f'answer:{o["id"]}:0:0'))


async def test_order_selection_retains_all_messages(app,s):
    old=s.new_order(1); s.set_order(old['id'],status='done')
    for i in range(1,4): await app.private(message(i,text='вопрос'))
    assert s.one('SELECT COUNT(*) n FROM queue WHERE oid IS NULL')['n']==3
    await app.dispatch_callback(cb(f'select:{old["id"]}'))
    assert s.pending(old['id'])==3
    assert s.order(old['id'])['status']=='clarify'


async def test_completion_waits_and_only_one_fact(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90,receipt=80)
    s.enqueue(message(text='x'),o['id'],'in',{'text':'x'})
    await app.dispatch_callback(cb(f'finish:{o["id"]}'))
    await app.tick(); await app.tick()
    assert s.order(o['id'])['finish']==0
    assert s.one('SELECT COUNT(*) n FROM seen_facts')['n']==1


async def test_dispatcher_real_routing(app,s):
    from aiogram import Bot, Dispatcher
    from aiogram.types import Update
    dp=Dispatcher(); dp.include_router(app.router)
    bot=Bot('123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi')
    try:
        for i in range(1,4):
            await dp.feed_update(bot,Update(update_id=i,message=message(i,text='Материалы')))
        assert s.one('SELECT COUNT(*) n FROM queue')['n']==3
        assert app.bot.send_message.await_count==1
    finally:
        await bot.session.close()


async def test_reply_does_not_clear_newer_question(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90,waiting=time.time()-100)
    s.enqueue(message(uid=99,text='Ответ'),o['id'],'out',{'text':'Ответ'})
    s.enqueue(message(2,text='Новый вопрос'),o['id'],'in',{'text':'Новый вопрос'})
    await app.deliver(s.next_batch())
    assert s.order(o['id'])['waiting'] is not None


async def test_deleted_source_reconstructed(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    m=message(text='Текст <&>'); s.enqueue(m,o['id'],'in',payload(m))
    app.bot.copy_message.side_effect=TelegramBadRequest(method=CopyMessage(chat_id=1,from_chat_id=1,message_id=1),message='message to copy not found')
    await app.tick()
    assert s.pending(o['id'])==0
    call=app.bot.send_message.call_args
    assert call.kwargs['text']=='Текст <&>' and call.kwargs['parse_mode'] is None


async def test_close_rejects_pending_messages(app,s):
    o=s.new_order(1); s.set_order(o['id'],thread=100,card=90)
    s.enqueue(message(text='x'),o['id'],'in',{'text':'x'})
    with pytest.raises(ValueError):
        await app.dispatch_callback(cb(f'confirm:{o["id"]}',uid=99,chat=-100123))
    assert s.order(o['id'])['status']=='new'


async def test_menu_preserves_form_and_selected(app,s):
    o=s.new_order(1); s.set_order(o['id'],step=3)
    await app.private(message(text='🏠 Главное меню'))
    assert s.order(o['id'])['step']==3
    assert s.user(1)['selected']==o['id']


async def test_failure_one_order_does_not_block_others(app,s):
    a=s.new_order(1); b=s.new_order(2)
    s.enqueue(message(text='a'),a['id'],'in',{'text':'a'})
    s.enqueue(message(uid=2,text='b'),b['id'],'in',{'text':'b'})
    s.execute("UPDATE queue SET state='failed' WHERE oid=?",(a['id'],))
    assert s.next_batch()[0]['oid']==b['id']
