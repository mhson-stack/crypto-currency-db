from database import get_coin_history, import_to_db
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, date, timedelta, timezone

sched = BlockingScheduler()


@sched.scheduled_job('cron', day_of_week='mon-sun', hour=0, minute=0, timezone='Asia/Seoul')
def scheduled_job():
    print('This job is run every day at 0 am.')
    symbols = ["BTC", "ETH", "ETC", "XRP", "BNB"]
    start = (datetime.now(timezone.utc) - timedelta(hours=14)).strftime("%Y-%m-%dT01:00+09:00")
    end = (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y-%m-%dT00:00+09:00")
    print(datetime.now())
    print(f"From {start} to {end}")

    for symbol in symbols:
        df = get_coin_history(symbol, start=start, end=end)
        import_to_db(symbol, df, method="append")

    print("DB Updated")

sched.start()