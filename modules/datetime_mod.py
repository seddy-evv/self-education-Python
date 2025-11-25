import datetime


def demo_datetime():
    # Current date and time
    now = datetime.datetime.now()
    print("Current datetime:", now)

    # Current date
    today = datetime.date.today()
    print("Today's date:", today)

    # Create a specific date
    d = datetime.date(2025, 11, 4)
    print("Specific date:", d)

    # Create a specific time
    t = datetime.time(14, 30, 15)
    print("Specific time:", t)

    # Combine date and time
    dt = datetime.datetime.combine(d, t)
    print("Combined datetime:", dt)

    # Parse date from string, str -> date
    dt_from_str = datetime.datetime.strptime('2025-11-04 14:30:15', '%Y-%m-%d %H:%M:%S')
    print("Parsed datetime:", dt_from_str)

    # Format datetime as string, date -> str
    formatted = now.strftime('%Y-%m-%d %H:%M:%S')
    print("Formatted datetime:", formatted)

    # Time difference (timedelta)
    delta = datetime.timedelta(days=5, hours=3)
    print("Timedelta:", delta)
    future = now + delta
    print("Future datetime:", future)

    # Timezone aware datetime
    tz = datetime.timezone(datetime.timedelta(hours=2))
    aware_dt = datetime.datetime.now(tz)
    print("Timezone-aware datetime:", aware_dt)


if __name__ == "__main__":
    demo_datetime()
