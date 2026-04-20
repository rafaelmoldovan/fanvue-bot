import os
import psycopg2

DATABASE_URL = os.environ.get('postgresql://postgres:kFKOUNUiQjgrpDhEHXbnqCpogvpmxjeL@postgres.railway.internal:5432/railway')

def get_db_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_database():
    conn = get_db_conn()
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            msg_id VARCHAR PRIMARY KEY,
            chat_id VARCHAR NOT NULL,
            fan_name VARCHAR,
            sender_uuid VARCHAR,
            text TEXT,
            timestamp TIMESTAMP,
            was_replied BOOLEAN DEFAULT FALSE,
            reply_text TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS fan_profiles (
            chat_id VARCHAR PRIMARY KEY,
            fan_name VARCHAR,
            handle VARCHAR,
            total_messages INTEGER DEFAULT 0,
            total_gifts REAL DEFAULT 0,
            last_interaction TIMESTAMP,
            fan_type VARCHAR DEFAULT 'new',
            inside_jokes TEXT DEFAULT '[]',
            meetup_ask_count INTEGER DEFAULT 0,
            content_ask_count INTEGER DEFAULT 0,
            last_reply_time TIMESTAMP,
            blocked BOOLEAN DEFAULT FALSE
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            key VARCHAR PRIMARY KEY,
            value TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS bot_stats (
            key VARCHAR PRIMARY KEY,
            value INTEGER DEFAULT 0
        )
    ''')
    
    c.execute('''
        INSERT INTO bot_stats (key, value) 
        VALUES ('messages_found', 0), ('replies_sent', 0)
        ON CONFLICT (key) DO NOTHING
    ''')
    
    conn.commit()
    c.close()
    conn.close()
    print("PostgreSQL tables ready")

if __name__ == '__main__':
    init_database()
