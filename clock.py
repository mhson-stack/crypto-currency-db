from database import get_coin_history, import_to_db
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, date, timedelta, timezone

sched = BlockingScheduler()


@sched.scheduled_job('cron', day_of_week='mon-sun', hour=0, minute=42, timezone='Asia/Seoul')
def scheduled_job():
    print('This job is run every weekday at 5pm.')
    symbols = ["BTC", "ETH", "ETC", "XRP", "BNB"]
    start = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:00")
    print(date.today())
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00")

    for symbol in symbols:
        df = get_coin_history(symbol, start=start, end=end)
        import_to_db(symbol, df, method="append")


sched.start()

