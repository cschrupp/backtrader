#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2020 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


import bisect
import collections
from datetime import date, datetime, timedelta, timezone
from itertools import islice

import schedule
import pandas as pd
import pandas_market_calendars as mcal
import pytz

from .feed import AbstractDataBase
from .metabase import MetaParams
from .utils import date2num, num2date
from .utils.py3 import integer_types, range, with_metaclass
from .utils import TIME_MAX


__all__ = ['SESSION_TIME', 'SESSION_START', 'SESSION_END', 'Timer']

SESSION_TIME, SESSION_START, SESSION_END = range(3)


class Timer(with_metaclass(MetaParams, object)):
    params = (
        ('tid', None),
        ('owner', None),
        ('strats', False),
        ('when', None),
        ('offset', timedelta()),
        ('repeat', timedelta()),
        ('weekdays', []),
        ('weekcarry', False),
        ('monthdays', []),
        ('monthcarry', True),
        ('allow', None),  # callable that allows a timer to take place
        ('tzdata', None),
        ('cheat', False),
    )

    SESSION_TIME, SESSION_START, SESSION_END = range(3)

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def start(self, data):
        # write down the 'reset when' value
        if not isinstance(self.p.when, integer_types):  # expect time/datetime
            self._rstwhen = self.p.when
            self._tzdata = self.p.tzdata
        else:
            self._tzdata = data if self.p.tzdata is None else self.p.tzdata

            if self.p.when == SESSION_START:
                self._rstwhen = self._tzdata.p.sessionstart
            elif self.p.when == SESSION_END:
                self._rstwhen = self._tzdata.p.sessionend

        self._isdata = isinstance(self._tzdata, AbstractDataBase)
        self._reset_when()

        self._nexteos = datetime.min
        self._curdate = date.min

        self._curmonth = -1  # non-existent month
        self._monthmask = collections.deque()

        self._curweek = -1  # non-existent week
        self._weekmask = collections.deque()

    def _reset_when(self, ddate=datetime.min):
        self._when = self._rstwhen
        self._dtwhen = self._dwhen = None

        self._lastcall = ddate

    def _check_month(self, ddate):
        if not self.p.monthdays:
            return True

        mask = self._monthmask
        daycarry = False
        dmonth = ddate.month
        if dmonth != self._curmonth:
            self._curmonth = dmonth  # write down new month
            daycarry = self.p.monthcarry and bool(mask)
            self._monthmask = mask = collections.deque(self.p.monthdays)

        dday = ddate.day
        dc = bisect.bisect_left(mask, dday)  # "left" for days before dday
        daycarry = daycarry or (self.p.monthcarry and dc > 0)
        if dc < len(mask):
            curday = bisect.bisect_right(mask, dday, lo=dc) > 0  # check dday
            dc += curday
        else:
            curday = False

        while dc:
            mask.popleft()
            dc -= 1

        return daycarry or curday

    def _check_week(self, ddate=date.min):
        if not self.p.weekdays:
            return True

        _, dweek, dwkday = ddate.isocalendar()

        mask = self._weekmask
        daycarry = False
        if dweek != self._curweek:
            self._curweek = dweek  # write down new month
            daycarry = self.p.weekcarry and bool(mask)
            self._weekmask = mask = collections.deque(self.p.weekdays)

        dc = bisect.bisect_left(mask, dwkday)  # "left" for days before dday
        daycarry = daycarry or (self.p.weekcarry and dc > 0)
        if dc < len(mask):
            curday = bisect.bisect_right(mask, dwkday, lo=dc) > 0  # check dday
            dc += curday
        else:
            curday = False

        while dc:
            mask.popleft()
            dc -= 1

        return daycarry or curday

    def check(self, dt):
        d = num2date(dt)
        ddate = d.date()
        if self._lastcall == ddate:  # not repeating, awaiting date change
            return False

        if d > self._nexteos:
            if self._isdata:  # eos provided by data
                nexteos, _ = self._tzdata._getnexteos()
            else:  # generic eos
                nexteos = datetime.combine(ddate, TIME_MAX)
            self._nexteos = nexteos
            self._reset_when()

        if ddate > self._curdate:  # day change
            self._curdate = ddate
            ret = self._check_month(ddate)
            if ret:
                ret = self._check_week(ddate)
            if ret and self.p.allow is not None:
                ret = self.p.allow(ddate)

            if not ret:
                self._reset_when(ddate)  # this day won't make it
                return False  # timer target not met

        # no day change or passed month, week and allow filters on date change
        dwhen = self._dwhen
        dtwhen = self._dtwhen
        if dtwhen is None:
            dwhen = datetime.combine(ddate, self._when)
            if self.p.offset:
                dwhen += self.p.offset

            self._dwhen = dwhen

            if self._isdata:
                self._dtwhen = dtwhen = self._tzdata.date2num(dwhen)
            else:
                self._dtwhen = dtwhen = date2num(dwhen, tz=self._tzdata)

        if dt < dtwhen:
            return False  # timer target not met

        self.lastwhen = dwhen  # record when the last timer "when" happened

        if not self.p.repeat:  # cannot repeat
            self._reset_when(ddate)  # reset and mark as called on ddate
        else:
            if d > self._nexteos:
                if self._isdata:  # eos provided by data
                    nexteos, _ = self._tzdata._getnexteos()
                else:  # generic eos
                    nexteos = datetime.combine(ddate, TIME_MAX)

                self._nexteos = nexteos
            else:
                nexteos = self._nexteos

            while True:
                dwhen += self.p.repeat
                if dwhen > nexteos:  # new schedule is beyone session
                    self._reset_when(ddate)  # reset to original point
                    break

                if dwhen > d:  # gone over current datetime
                    self._dtwhen = dtwhen = date2num(dwhen)  # float timestamp
                    # Get the localized expected next time
                    if self._isdata:
                        self._dwhen = self._tzdata.num2date(dtwhen)
                    else:  # assume pytz compatible or None
                        self._dwhen = num2date(dtwhen, tz=self._tzdata)

                    break

        return True  # timer target was met


class ResetTimer(with_metaclass(MetaParams, object)):
    """
    import schedule
    import time

    def job():
        print("I'm working...")

    # Run job every 3 second/minute/hour/day/week,
    # Starting 3 second/minute/hour/day/week from now
    schedule.every(3).seconds.do(job)
    schedule.every(3).minutes.do(job)
    schedule.every(3).hours.do(job)
    schedule.every(3).days.do(job)
    schedule.every(3).weeks.do(job)

    # Run job every minute at the 23rd second
    schedule.every().minute.at(":23").do(job)

    # Run job every hour at the 42rd minute
    schedule.every().hour.at(":42").do(job)

    # Run jobs every 5th hour, 20 minutes and 30 seconds in.
    # If current time is 02:00, first execution is at 06:20:30
    schedule.every(5).hours.at("20:30").do(job)

    # Run job every day at specific HH:MM and next HH:MM:SS
    schedule.every().day.at("10:30").do(job)
    schedule.every().day.at("10:30:42").do(job)

    # Run job on a specific day of the week
    schedule.every().monday.do(job)
    schedule.every().wednesday.at("13:15").do(job)
    schedule.every().minute.at(":17").do(job)

    while True:
    schedule.run_pending()
    time.sleep(1)
    """
    params = (
        ('tid', None),
        ('owner', None),
        ('strats', False),
        ('reset_time', None),
        ('live_test', 1),
        ('cycle_mult', 2),
        ('market', "stock"),
        ('early_trading', False),
        ('late_trading', False),
        ('strategy', None),
        ('allow', None),  # callable that allows a timer to take place
        ('tzdata', None),
        ('json_handler', None),  # Injected JSON file handler
    )
    def __init__(self, *args, **kwargs):

        # Use injected JSON handler
        self.jsonfile = self.p.json_handler
        if self.jsonfile is None:
            raise ValueError("json_handler parameter is required for ResetTimer")

        self.args = args
        self.kwargs = kwargs

        self.reset_time = self.p.reset_time
        self.live_test = self.p.live_test
        self.cycle_mult = self.p.cycle_mult
        self.market = self.p.market
        self.early_trading = self.p.early_trading
        self.late_trading = self.p.late_trading
        self.strategy = self.p.strategy
        self.tz = self.p.tzdata

        self.lastwhen = None

        self.timer=None

        for t in self.reset_time:
            schedule.every().day.at(t).do(self.daily_reset).tag('Daily reset', 'Fixed Time')

        schedule.every(self.live_test).minutes.do(self.live_test_app).tag('Periodic reset', 'Minutely')

        print("Watchdog schedule:", schedule.get_jobs())


    def market_open(self):

        # Create a calendar
        nyse = mcal.get_calendar('NYSE')
        cme = mcal.get_calendar("CME_Equity")

        market = self.market
        timezone = self.tz
        early_trading = self.early_trading
        late_trading = self.late_trading
        rth = not any([early_trading, late_trading])
        market_times = ["market_open", "market_close"]

        if early_trading:
            market_times.append("pre")
        if late_trading:
            market_times.append("post")

        today = date.today()
        now = pd.Timestamp(datetime.now(), tz=timezone)

        if market == "stock":
            stock = nyse.schedule(start_date=today - timedelta(days=7), end_date=today + timedelta(days=7), market_times=market_times,
                                  tz=timezone)
            try:
                is_open = nyse.open_at_time(stock, now, only_rth=rth)
            except ValueError:
                is_open = False
            return is_open

        elif market == "futures":
            futures = cme.schedule(start_date=today - timedelta(days=7), end_date=today + timedelta(days=7), tz=timezone)
            try:
                is_open = cme.open_at_time(futures, now)
            except ValueError:
                is_open = False
            return is_open

        elif market == "both":
            stock = nyse.schedule(start_date=today - timedelta(days=7), end_date=today + timedelta(days=7), market_times=market_times,
                                  tz=timezone)
            futures = cme.schedule(start_date=today - timedelta(days=7), end_date=today + timedelta(days=7), tz=timezone)
            both = mcal.merge_schedules(schedules=[stock, futures], how='outer')
            try:
                is_open = nyse.open_at_time(stock, now, only_rth=rth)
            except ValueError:
                is_open = False
            return is_open

    def daily_reset(self):

        self.timer = True
        print("Undergoing daily reset...")

    def live_test_app(self):

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        strategy = self.strategy
        last_cycle, filepath = self.jsonfile.readValue("strategy", strategy, "LASTCYCLE")
        candles, filepath = self.jsonfile.readValue("strategy", strategy, "CANDLES")
        long_candles, filepath = self.jsonfile.readValue("strategy", strategy, "LONGCANDLES")
        now = datetime.now()
        cycle = datetime.strptime(last_cycle, "%Y-%m-%d %H:%M:%S")
        cycle_mult = self.cycle_mult

        num = int(candles.split(' ')[0])
        val = candles.split(' ')[1]


        if val == "sec" or val == "secs":
            num = timedelta(seconds=num)
        elif val == "min" or val == "mins":
            num = timedelta(minutes=num)
        elif val == "hour" or val == "hours":
            num = timedelta(hours=num)
        elif val == "day" or val == "days":
            num = timedelta(days=num)
        elif val == "week" or val == "weeks":
            num = timedelta(weeks=num)
        elif val == "month" or val == "months":
            num = timedelta(days=30 * num)

        self.lastwhen = now - cycle

        if self.market_open():

            timecheck = self.lastwhen >  cycle_mult * num
            print("Market Open... watchdog timestamp:", now, "Elapsed time since last cycle:", self.lastwhen)

            if timecheck:
                print("Frozen cycles detected...reseting")
                self.timer = True
        else:
            print(" Market Closed... watchdog timestamp:", now, "Elapsed time since last cycle:", self.lastwhen)

    def check(self):

        schedule.run_pending()

        if self.timer:
            self.timer = False
            return True

        return False