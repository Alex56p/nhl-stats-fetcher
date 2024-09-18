from nhlpy import NHLClient
from datetime import datetime

client = NHLClient()


def get_season_from_date(date: datetime):
    schedule = client.schedule.get_schedule(date.strftime('%Y-%m-%d'))
    year = datetime.strptime(schedule["preSeasonStartDate"], "%Y-%m-%d").year
    return str(year) + str(year + 1)
