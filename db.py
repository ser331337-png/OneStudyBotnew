"""Durable SQLite state. All SQL is local and short; one process owns the bot."""
import json
import random
import sqlite3
import time
from pathlib import Path

TERMINAL = ('done', 'cancelled', 'archive')


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.c = sqlite3.connect(path)
        self.c.row_factory = sqlite3.Row
        self.c.execute('PRAGMA journal_mode=WAL')
        self.c.execute('PRAGMA foreign_keys=ON')
        self.c.execute('PRAGMA busy_timeout=5000')
        self.c.executescript('''
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY, name TEXT, username TEXT, selected INTEGER,
          greeted INTEGER DEFAULT 0, blocked INTEGER DEFAULT 0, selector INTEGER);
        CREATE TABLE IF NOT EXISTS orders(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER REFERENCES users(id),
          thread INTEGER UNIQUE, status TEXT DEFAULT 'new', work TEXT DEFAULT '',
          subject TEXT DEFAULT '', deadline TEXT DEFAULT '', anti TEXT DEFAULT '',
          title TEXT DEFAULT '', step INTEGER DEFAULT -1, form_msg INTEGER,
          card INTEGER, photo INTEGER DEFAULT 0, receipt INTEGER,
          batch INTEGER DEFAULT 0, finish INTEGER DEFAULT 0, fact TEXT,
          rating INTEGER, review INTEGER DEFAULT 0,
          created REAL, activity REAL, waiting REAL, reminded INTEGER DEFAULT 0,
          warned INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS queue(
          id INTEGER PRIMARY KEY, src INTEGER, mid INTEGER, uid INTEGER,
          oid INTEGER REFERENCES orders(id), direction TEXT, payload TEXT,
          album TEXT, state TEXT DEFAULT 'pending', tries INTEGER DEFAULT 0,
          due REAL DEFAULT 0, alerted INTEGER DEFAULT 0, error TEXT,
          created REAL, UNIQUE(src,mid));
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY, oid INTEGER, action TEXT, created REAL);
        CREATE TABLE IF NOT EXISTS seen_facts(uid INTEGER, idx INTEGER,
          PRIMARY KEY(uid,idx));
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE INDEX IF NOT EXISTS queue_due ON queue(state,due);
        ''')
        self.c.execute("UPDATE queue SET state='pending' WHERE state='sending'")
        self.migrate()
        self.c.commit()

    def execute(self, sql, args=()):
        with self.c:
            return self.c.execute(sql, args)

    def one(self, sql, args=()):
        r = self.c.execute(sql, args).fetchone()
        return dict(r) if r else None

    def all(self, sql, args=()):
        return [dict(r) for r in self.c.execute(sql, args).fetchall()]

    def migrate(self):
        if self.c.execute("SELECT 1 FROM meta WHERE key='migration_v3'").fetchone():
            return
        exists = self.c.execute("SELECT 1 FROM sqlite_master WHERE name='threads'").fetchone()
        if exists:
            for r in self.c.execute('SELECT * FROM threads').fetchall():
                d = dict(r)
                self.c.execute('INSERT OR IGNORE INTO users(id,name) VALUES(?,?)',
                               (d['user_id'], 'Клиент'))
                cur = self.c.execute('''INSERT OR IGNORE INTO orders
                  (user_id,thread,status,title,rating,created,activity)
                  VALUES(?,?,?,?,?,?,?)''', (d['user_id'], d['thread_id'],
                  'done' if d.get('status') == 'done' else 'working',
                  d.get('topic_name') or 'Перенесённый заказ', d.get('rating'),
                  time.time(), time.time()))
                if cur.rowcount:
                    self.c.execute('UPDATE users SET selected=? WHERE id=?',
                                   (cur.lastrowid, d['user_id']))
        self.c.execute("INSERT INTO meta VALUES('migration_v3','1')")

    def user(self, uid):
        return self.one('SELECT * FROM users WHERE id=?', (uid,))

    def upsert_user(self, u):
        self.execute('''INSERT INTO users(id,name,username) VALUES(?,?,?)
          ON CONFLICT(id) DO UPDATE SET name=excluded.name,username=excluded.username''',
          (u.id, u.full_name, u.username))

    def order(self, oid):
        return self.one('SELECT * FROM orders WHERE id=?', (oid,))

    def orders(self, uid):
        return self.all('SELECT * FROM orders WHERE user_id=? ORDER BY id DESC', (uid,))

    def set_order(self, oid, **fields):
        allowed = {'thread','status','work','subject','deadline','anti','title','step',
                   'form_msg','card','photo','receipt','batch','finish','fact','rating',
                   'review','activity','waiting','reminded','warned'}
        if not fields or not set(fields) <= allowed:
            raise ValueError('Invalid order fields')
        self.execute('UPDATE orders SET '+','.join(k+'=?' for k in fields)+' WHERE id=?',
                     (*fields.values(), oid))

    def new_order(self, uid):
        now = time.time()
        with self.c:
            oid = self.c.execute('INSERT INTO orders(user_id,created,activity) VALUES(?,?,?)',
                                 (uid, now, now)).lastrowid
            self.c.execute('UPDATE users SET selected=?,selector=NULL WHERE id=?', (oid, uid))
        self.attach_pending(uid, oid)
        return self.order(oid)

    def select(self, uid, oid):
        o = self.order(oid)
        if not o or o['user_id'] != uid:
            raise ValueError('Order ownership')
        if o['status'] in TERMINAL:
            self.set_order(oid, status='clarify', activity=time.time(), waiting=None,
                           reminded=0, warned=0)
            self.event(oid, 'Повторное обращение')
        self.execute('UPDATE users SET selected=?,selector=NULL WHERE id=?', (oid,uid))
        self.attach_pending(uid, oid)

    def event(self, oid, action):
        self.execute('INSERT INTO events(oid,action,created) VALUES(?,?,?)',
                     (oid,action,time.time()))

    def attach_pending(self, uid, oid):
        with self.c:
            r = self.c.execute("SELECT MIN(created) FROM queue WHERE uid=? AND oid IS NULL AND direction='in'",(uid,)).fetchone()[0]
            self.c.execute("UPDATE queue SET oid=? WHERE uid=? AND oid IS NULL AND direction='in'",(oid,uid))
            if r:
                self.c.execute('UPDATE orders SET waiting=COALESCE(waiting,?),activity=?,batch=1 WHERE id=?',
                               (r,time.time(),oid))

    def enqueue(self, message, oid, direction, payload):
        now = time.time()
        with self.c:
            cur = self.c.execute('''INSERT OR IGNORE INTO queue
              (src,mid,uid,oid,direction,payload,album,created,due)
              VALUES(?,?,?,?,?,?,?,?,?)''', (message.chat.id,message.message_id,
              message.from_user.id,oid,direction,json.dumps(payload,ensure_ascii=False),
              message.media_group_id,now,now+(2 if message.media_group_id else 0)))
            if cur.rowcount and oid and direction == 'in':
                self.c.execute('''UPDATE orders SET waiting=COALESCE(waiting,?),
                  activity=?,warned=0,batch=1 WHERE id=?''',(now,now,oid))
            return bool(cur.rowcount)

    def next_batch(self):
        # Preserve order per destination. A failed order cannot block other clients.
        r = self.one('''SELECT q.* FROM queue q WHERE state='pending' AND oid IS NOT NULL
          AND due<=? AND NOT EXISTS(SELECT 1 FROM queue p WHERE p.id<q.id
            AND p.oid=q.oid AND p.direction=q.direction AND p.state!='sent')
          ORDER BY id LIMIT 1''',(time.time(),))
        if not r:
            return []
        if not r['album']:
            return [r]
        rows = self.all("SELECT * FROM queue WHERE src=? AND album=? AND oid=? AND direction=? AND state='pending' ORDER BY mid LIMIT 10",
                        (r['src'],r['album'],r['oid'],r['direction']))
        return rows if max(x['due'] for x in rows)<=time.time() else []

    def pending(self, oid):
        return self.one("SELECT COUNT(*) n FROM queue WHERE oid=? AND direction='in' AND state!='sent'",(oid,))['n']

    def fact(self, uid, facts):
        seen = {r['idx'] for r in self.all('SELECT idx FROM seen_facts WHERE uid=?',(uid,))}
        choices = [i for i in range(len(facts)) if i not in seen]
        if not choices:
            self.execute('DELETE FROM seen_facts WHERE uid=?',(uid,))
            choices = list(range(len(facts)))
        idx = random.choice(choices)
        self.execute('INSERT INTO seen_facts VALUES(?,?)',(uid,idx))
        return facts[idx]

    def backup(self, folder, keep=14):
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / time.strftime('threads_%Y%m%d_%H%M%S.db',time.gmtime())
        with sqlite3.connect(path) as target:
            self.c.backup(target)
        for old in sorted(folder.glob('threads_*.db'))[:-keep]:
            old.unlink()
        return path
