import streamlit as st
from kafka import KafkaProducer
import psycopg2
import bcrypt
import json
import re
from datetime import datetime

# PostgreSQL setup
try:
    conn = psycopg2.connect(
        dbname="postgres",  # Database where the tables are created
        user="postgres",
        password="password123",
        host="localhost",
        port="5433"
    )
    cursor = conn.cursor()
    print("Connected to the database successfully!")  # Debugging information

    # Verifying the current database
    cursor.execute("SELECT current_database();")
    current_db = cursor.fetchone()
    print(f"Connected to database: {current_db[0]}")  # Should print 'kafka_app'

    # Verifying that the required tables exist in the 'public' schema
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()
    print("Tables in the public schema:", tables)  # Debugging information to list all tables
except Exception as e:
    print(f"Database connection error: {e}")
    st.error("Failed to connect to the database. Please check your database settings.")

# Kafka setup
KAFKA_TOPIC = "login-events"
KAFKA_BROKER = "localhost:9092"

# Creating Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def is_valid_password(password):
    """Utility function to enforce password rules"""
    if (len(password) >= 8 and
            re.search(r"[A-Z]", password) and
            re.search(r"[a-z]", password) and
            re.search(r"\d", password) and
            re.search(r"[#?!@$%^&*-]", password)):
        return True
    return False


def is_valid_username(username):
    """Utility function to enforce username rules"""
    if re.match(r"^[a-zA-Z0-9]{4,15}$", username):
        return True
    return False


def register_user(username, password):
    """Registration function"""
    # Check if username already exists in PostgreSQL
    cursor.execute("SELECT * FROM public.users WHERE username = %s", (username,))
    user_exists = cursor.fetchone()

    if user_exists:
        return False, "Username already exists."

    if not is_valid_username(username):
        return False, "Invalid username. Must be 4-15 characters long and contain only letters and numbers."

    if not is_valid_password(password):
        return False, ("Invalid password. Must be at least 8 characters long and contain an uppercase letter, "
                       "a lowercase letter, a number, and a special character.")

    # Hashing the password for security and storing the user in PostgreSQL
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    cursor.execute(
        "INSERT INTO public.users (username, password) VALUES (%s, %s)",
        (username, hashed_password)
    )
    conn.commit()
    print(f"Registered User - Username: {username}, Hashed Password: {hashed_password}")  # Debug information

    # Tracking the event of a new user being created
    track_new_user_created(username)

    return True, "User registered successfully!"


def login(username, password):
    """Login function"""
    # Retrieving user details from PostgreSQL
    cursor.execute("SELECT password FROM public.users WHERE username = %s", (username,))
    user_data = cursor.fetchone()
    print(f"Logging In - Username: {username}, User Data from DB: {user_data}")  # Debug information

    if user_data:
        stored_password = user_data[0].encode('utf-8')
        print(f"Stored Hashed Password: {stored_password}")  # Debug information
        print(f"Provided Password: {password}")  # Debug information
        print(f"Encoded Password for Comparison: {password.encode('utf-8')}")  # Debug information

        # Check if the provided password matches the stored hashed password
        if bcrypt.checkpw(password.encode('utf-8'), stored_password):
            print("Password match confirmed!")  # Debug information
            return True
        else:
            print("Password mismatch")  # Debug information
    else:
        print(f"Username {username} not found")  # Debug information

    return False


def track_incorrect_login(username):
    """Track incorrect logins"""
    event = {
        "username": username,
        "event": "incorrect_password",
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(KAFKA_TOPIC, value=event)

    # Save to PostgreSQL
    cursor.execute(
        "INSERT INTO public.events (username, event_type) VALUES (%s, %s)",
        (username, "incorrect_password")
    )
    conn.commit()
    print(f"Logged Incorrect Login Event - {event}")  # Debug information


def track_new_user_created(username):
    """Track new user creation"""
    event = {
        "username": username,
        "event": "new_user_created",
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(KAFKA_TOPIC, value=event)

    # Save to PostgreSQL
    cursor.execute(
        "INSERT INTO public.events (username, event_type) VALUES (%s, %s)",
        (username, "new_user_created")
    )
    conn.commit()
    print(f"New User Created Event - Username: {username}")  # Debug information


# Streamlit UI
st.title("User Registration and Login System with Kafka and PostgreSQL Integration")

# Sidebar Navigation
page = st.sidebar.selectbox("Choose Action", ["Login", "Register"])

if page == "Register":
    st.subheader("Create a New Account")
    with st.form("Register"):
        # Add tooltips for username and password fields
        new_username = st.text_input(
            "Username",
            help="Must be 4-15 characters long and contain only letters and numbers."
        )
        new_password = st.text_input(
            "Password",
            type="password",
            help="Must be at least 8 characters long and contain an uppercase letter, "
                 "a lowercase letter, a number, and a special character."
        )
        register = st.form_submit_button("Register")

        if register:
            success, message = register_user(new_username, new_password)
            if success:
                st.success(message)
            else:
                st.error(message)

elif page == "Login":
    st.subheader("Login to Your Account")
    with st.form("Login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

        if submit:
            if login(username, password):
                st.success(f"Welcome {username}!")
            else:
                st.error("Incorrect username or password")
                track_incorrect_login(username)


# Display information about incorrect logins being sent to Kafka
st.markdown("""
### Kafka and PostgreSQL Integration
This app sends events for incorrect login attempts and user creation to an Apache Kafka topic called `login-events`. 
It also stores user data and events in PostgreSQL. You can use a Kafka consumer to monitor these events in real-time.
""")
