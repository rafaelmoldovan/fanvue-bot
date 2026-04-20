import sqlite3
import os

DB_FILE = 'bot_data.db'

def init_database():
    """Create SQLite database with all needed tables"""
    
    # Remove old DB if corrupted (optional — remove this line if you want to keep data)
    # if os.path.exists(DB_FILE):
    #     os.remove(DB_FILE)
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Messages table — stores full conversation history
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            msg_id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            fan_name TEXT,
            sender_uuid TEXT,
            text TEXT,
            timestamp TEXT,
            was_replied BOOLEAN DEFAULT 0,
            reply_text TEXT
        )
    ''')
    
    # Fan profiles — tracks each fan's relationship level
    c.execute('''
        CREATE TABLE IF NOT EXISTS fan_profiles (
            chat_id TEXT PRIMARY KEY,
            fan_name TEXT,
            handle TEXT,
            total_messages INTEGER DEFAULT 0,
            total_gifts REAL DEFAULT 0,
            last_interaction TEXT,
            fan_type TEXT DEFAULT 'new',
            inside_jokes TEXT DEFAULT '[]',
            meetup_ask_count INTEGER DEFAULT 0,
            content_ask_count INTEGER DEFAULT 0,
            last_reply_time TEXT,
            blocked BOOLEAN DEFAULT 0
        )
    ''')
    
    # Token storage — survives redeploys
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    # Bot stats — persistent counters
    c.execute('''
        CREATE TABLE IF NOT EXISTS bot_stats (
            key TEXT PRIMARY KEY,
            value INTEGER DEFAULT 0
        )
    ''')
    
    # Initialize stats
    c.execute("INSERT OR IGNORE INTO bot_stats (key, value) VALUES ('messages_found', 0)")
    c.execute("INSERT OR IGNORE INTO bot_stats (key, value) VALUES ('replies_sent', 0)")
    
    conn.commit()
    conn.close()
    print(f"✓ Database initialized: {DB_FILE}")
    print("Tables created: messages, fan_profiles, tokens, bot_stats")

if __name__ == '__main__':
    init_database()
    print("\nRun this once. Then start your bot.")
