import datetime


def demo_datetime():
    # Current date and time
    now = datetime.datetime.now()
    print("Current datetime:", now)
    # Current datetime: 2025-11-29 10:00:36.204181

    # Current date
    today = datetime.date.today()
    print("Today's date:", today)
    # Today's date: 2025-11-29

    # Create a specific date
    d = datetime.date(2025, 11, 4)
    print("Specific date:", d)
    # Specific date: 2025-11-04

    # Create a specific time
    t = datetime.time(14, 30, 15)
    print("Specific time:", t)
    # Specific time: 14:30:15

    # Combine date and time
    dt = datetime.datetime.combine(d, t)
    print("Combined datetime:", dt)
    # Combined datetime: 2025-11-04 14:30:15

    # Parse date from string, str -> date
    dt_from_str = datetime.datetime.strptime('2025-11-04 14:30:15', '%Y-%m-%d %H:%M:%S')
    print("Parsed datetime:", dt_from_str)
    # Parsed datetime: 2025-11-04 14:30:15

    # Format datetime as string, date -> str
    formatted = now.strftime('%Y-%m-%d %H:%M:%S')
    print("Formatted datetime:", formatted)
    # Formatted datetime: 2025-11-29 10:00:36

    # Time difference (timedelta)
    delta = datetime.timedelta(days=5, hours=3)
    print("Timedelta:", delta)
    # Timedelta: 5 days, 3:00:00
    future = now + delta
    print("Future datetime:", future)
    # Future datetime: 2025-12-04 13:00:36.204181

    # Timezone aware datetime
    tz = datetime.timezone(datetime.timedelta(hours=2))
    aware_dt = datetime.datetime.now(tz)
    print("Timezone-aware datetime:", aware_dt)
    # Timezone-aware datetime: 2025-11-29 09:00:36.219991+02:00


if __name__ == "__main__":
    demo_datetime()
