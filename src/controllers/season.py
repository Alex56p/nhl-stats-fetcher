from nhlpy import NHLClient
from datetime import datetime

client = NHLClient()


def get_current_season():
    todaySchedule = client.schedule.get_schedule()
    year = datetime.strptime(todaySchedule["preSeasonStartDate"], "%Y-%m-%d").year
    return str(year) + str(year + 1)
