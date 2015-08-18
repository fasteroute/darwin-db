from darwindb.postgres.BaseStore import BaseStore
from darwindb.utils import timezone_for_date_and_time

from collections import OrderedDict

from datetime import datetime, timedelta
from dateutil.parser import parse

import pytz

class ScheduleMessageStore(BaseStore):

    table_schedule_name = "schedule"
    table_schedule_fields = OrderedDict([
            ("rid", "varchar PRIMARY KEY NOT NULL"),
            ("uid", "varchar NOT NULL"),
            ("headcode", "varchar NOT NULL"),
            ("start_date", "date NOT NULL"),
            ("toc_code", "varchar"),
            ("passenger_service", "boolean"),
            ("status", "varchar"),
            ("category", "varchar"),
            ("active", "boolean"),
            ("deleted", "boolean"),
            ("charter", "boolean"),
            ("cancellation_reason_code", "integer"),
            ("cancellation_reason_tiploc", "varchar"),
            ("cancellation_reason_near", "boolean"),
            ("reverse_formation", "boolean"),
            ("late_reason_code", "integer"),
            ("late_reason_tiploc", "varchar"),
            ("late_reason_near", "boolean"),
    ])
    
    table_schedule_location_name = "schedule_location"
    table_schedule_location_fields = OrderedDict([
            ("id", "bigserial PRIMARY KEY NOT NULL"),
            ("rid", "varchar REFERENCES schedule (rid)"),
            ("type", "varchar NOT NULL"),
            ("position", "smallint NOT NULL"),
            ("tiploc", "varchar NOT NULL"),
            ("activity_codes", "varchar"),
            ("planned_activity_codes", "varchar"),
            ("cancelled", "boolean"),
            ("false_tiploc", "varchar"),
            ("route_delay", "integer"),
            ("working_arrival_time", "timestamp with time zone"),
            ("public_arrival_time", "timestamp with time zone"),
            ("working_pass_time", "timestamp with time zone"),
            ("public_departure_time", "timestamp with time zone"),
            ("working_departure_time", "timestamp with time zone"),
            ("raw_working_arrival_time", "time"),
            ("raw_public_arrival_time", "time"),
            ("raw_working_pass_time", "time"),
            ("raw_public_departure_time", "time"),
            ("raw_working_departure_time", "time"),
            ("suppressed", "boolean"),
            ("length", "varchar"),
            ("detach_front", "boolean"),
            ("platform_suppressed", "boolean"),
            ("platform_suppressed_by_cis", "boolean"),
            ("platform_source", "varchar"),
            ("platform_confirmed", "boolean"),
            ("platform_number", "varchar"),
            ("forecast_arrival_estimated_time", "time with time zone"),
            ("forecast_arrival_working_estimated_time", "time with time zone"),
            ("forecast_arrival_actual_time", "time with time zone"),
            ("forecast_arrival_actual_time_reomved", "boolean"),
            ("forecast_arrival_manual_estimate_lower_limit_minutes", "integer"),
            ("forecast_arrival_manual_estimate_unknown_delay", "boolean"),
            ("forecast_arrival_unknown_delay", "boolean"),
            ("forecast_arrival_source", "varchar"),
            ("forecast_arrival_source_cis", "varchar"),
            ("forecast_departure_estimated_time", "time with time zone"),
            ("forecast_departure_working_estimated_time", "time with time zone"),
            ("forecast_departure_actual_time", "time with time zone"),
            ("forecast_departure_actual_time_reomved", "boolean"),
            ("forecast_departure_manual_estimate_lower_limit_minutes", "integer"),
            ("forecast_departure_manual_estimate_unknown_delay", "boolean"),
            ("forecast_departure_unknown_delay", "boolean"),
            ("forecast_departure_source", "varchar"),
            ("forecast_departure_source_cis", "varchar"),
            ("forecast_pass_estimated_time", "time with time zone"),
            ("forecast_pass_working_estimated_time", "time with time zone"),
            ("forecast_pass_actual_time", "time with time zone"),
            ("forecast_pass_actual_time_reomved", "boolean"),
            ("forecast_pass_manual_estimate_lower_limit_minutes", "integer"),
            ("forecast_pass_manual_estimate_unknown_delay", "boolean"),
            ("forecast_pass_unknown_delay", "boolean"),
            ("forecast_pass_source", "varchar"),
            ("forecast_pass_source_cis", "varchar"),
    ])

    def __init__(self, connection):
        super().__init__(connection)

        self.insert_schedule_query = "INSERT into {} ({}) VALUES({})".format(
                self.table_schedule_name,
                ", ".join(["{}".format(k) for k, v in list(self.table_schedule_fields.items())[0:14]]),
                ", ".join(["%s" for n in range(0, 14)]))
        
        self.insert_schedule_location_query = "INSERT into {} ({}) VALUES({})".format(
                self.table_schedule_location_name,
                ", ".join(["{}".format(k) for k, v in list(self.table_schedule_location_fields.items())[1:20]]),
                ", ".join(["%s" for n in range(0, 19)]))

        self.update_schedule_query = "UPDATE {} SET {} WHERE {}".format(
                self.table_schedule_name,
                ", ".join(["{}=%s".format(k) for k, v in list(self.table_schedule_fields.items())[1:14]]),
                "rid=%s")

        self.cursor = None

    def create_tables(self):
        cursor = self.connection.cursor()
        
        schedule_query = "CREATE TABLE IF NOT EXISTS {} ({});".format(self.table_schedule_name,
                ", ".join(["{} {}".format(k, v) for k, v in self.table_schedule_fields.items()]))
        
        schedule_locations_query = "CREATE TABLE IF NOT EXISTS {} ({});".format(self.table_schedule_location_name,
                ", ".join(["{} {}".format(k, v) for k, v in self.table_schedule_location_fields.items()]))

        cursor.execute(schedule_query)
        cursor.execute(schedule_locations_query)
        
        self.connection.commit()
        
        cursor.close()

    def save_schedule_message(self, message):
        if self.cursor is None:
            self.cursor = self.connection.cursor()

        # Calculate the date-times on the schedule message.
        self.build_sanitised_times(message)

        # Check if the schedule message is already found with that RTTI ID
        self.cursor.execute("SELECT rid FROM schedule WHERE rid=%s", (message["rid"],));

        #print("*** Saving Schedule Message.")

        # Check if this is a new Schedule.
        if self.cursor.rowcount == 0:
            self.cursor.execute(self.insert_schedule_query, (
                message["rid"],
                message["uid"],
                message["headcode"],
                message["start_date"],
                message["toc_code"],
                message["passenger_service"],
                message["status"],
                message["category"],
                message["active"],
                message["deleted"],
                message["charter"],
                message["cancellation_reason"]["code"] if message.get("cancellation_reason", None) is not None else None,
                message["cancellation_reason"].get("tiploc", None) if message.get("cancellation_reason", None) is not None else None,
                message["cancellation_reason"].get("near", None) if message.get("cancellation_reason", None) is not None else None,
            ))
        else:
           self.cursor.execute(self.update_schedule_query, (
                message["uid"],
                message["headcode"],
                message["start_date"],
                message["toc_code"],
                message["passenger_service"],
                message["status"],
                message["category"],
                message["active"],
                message["deleted"],
                message["charter"],
                message["cancellation_reason"]["code"] if message.get("cancellation_reason", None) is not None else None,
                message["cancellation_reason"].get("tiploc", None) if message.get("cancellation_reason", None) is not None else None,
                message["cancellation_reason"].get("near", None) if message.get("cancellation_reason", None) is not None else None,
                message["rid"],
            ))
           # FIXME: Save forecast components before deleting...
           self.cursor.execute("DELETE from {} WHERE rid=%s".format(self.table_schedule_location_name), (message["rid"],));
        i = 0
        for p in message["locations"]:
            self.cursor.execute(self.insert_schedule_location_query, (
                message["rid"],
                p["location_type"],
                i,
                p.get("tiploc", None),
                p.get("activity_codes", None),
                p.get("planned_activity_codes", None),
                p.get("cancelled", None),
                p.get("false_tiploc", None),
                p.get("route_delay", None),
                p.get("working_arrival_time", None),
                p.get("public_arrival_time", None),
                p.get("working_pass_time", None),
                p.get("public_departure_time", None),
                p.get("working_departure_time", None),
                p.get("raw_working_arrival_time", None),
                p.get("raw_public_arrival_time", None),
                p.get("raw_working_pass_time", None),
                p.get("raw_public_departure_time", None),
                p.get("raw_working_departure_time", None),
            ))
            i += 1
        
        self.connection.commit()

    def build_sanitised_times(self, message):
        # Convert the start date to a date.
        message["start_date"] = parse(message["start_date"]).date()

        #Convert the times into time objects.
        for l in message["locations"]:
            if l.get("working_arrival_time", None) is not None:
                l["raw_working_arrival_time"] = parse(l["working_arrival_time"]).time()
            else:
                l["raw_working_arrival_time"] = None
            if l.get("working_pass_time", None) is not None:
                l["raw_working_pass_time"] = parse(l["working_pass_time"]).time()
            else:
                l["raw_working_pass_time"] = None
            if l.get("working_departure_time", None) is not None:
                l["raw_working_departure_time"] = parse(l["working_departure_time"]).time()
            else:
                l["raw_working_departure_time"] = None
            if l.get("public_arrival_time", None) is not None:
                l["raw_public_arrival_time"] = parse(l["public_arrival_time"]).time()
            else:
                l["raw_public_arrival_time"] = None
            if l.get("public_departure_time", None) is not None:
                l["raw_public_departure_time"] = parse(l["public_departure_time"]).time()
            else:
                l["raw_public_departure_time"] = None

        # Get the first time.
        first_location = message["locations"][0]
        if first_location.get("raw_working_arrival_time", None) is not None:
            t = first_location["raw_working_arrival_time"]
        elif first_location.get("raw_working_pass_time", None) is not None:
            t = first_location["raw_working_pass_time"]
        elif first_location.get("raw_working_departure_time", None) is not None:
            t = first_location["raw_working_departure_time"]
        else:
            raise Exception()

        tz = timezone_for_date_and_time(message["start_date"], t)

        day_incrementor = 0
        o = None
        for p in message["locations"]:
            day_incrementor = self.build_times(day_incrementor, o, p, message["start_date"], tz)
            o = p

    def build_times(self, day_incrementor, last_location, this_location, start_date, tz):

        # Construct all the date/time objects iteratively, checking for back-in-time movement of
        # any of them.
        last_time = None
        if last_location is not None:
            last_time = self.get_last_time(last_location)

        if this_location["raw_working_arrival_time"] is not None:
            t = this_location["raw_working_arrival_time"]
            if last_time is not None:
                if last_time > t:
                    day_incrementor += 1
            d = start_date + timedelta(days=day_incrementor)
            this_location["working_arrival_time"] = tz.localize(datetime.combine(d, t)).astimezone(pytz.utc)
        else:
            this_location["working_arrival_time"] = None

        if this_location["raw_public_arrival_time"] is not None:
            t = this_location["raw_public_arrival_time"]
            if last_time is not None:
                if last_time > t:
                    day_incrementor += 1
            d = start_date + timedelta(days=day_incrementor)
            this_location["public_arrival_time"] = tz.localize(datetime.combine(d, t)).astimezone(pytz.utc)
        else:
            this_location["public_arrival_time"] = None

        if this_location["raw_working_pass_time"] is not None:
            t = this_location["raw_working_pass_time"]
            if last_time is not None:
                if last_time > t:
                    day_incrementor += 1
            d = start_date + timedelta(days=day_incrementor)
            this_location["working_pass_time"] = tz.localize(datetime.combine(d, t)).astimezone(pytz.utc)
        else:
            this_location["working_pass_time"] = None

        if this_location["raw_public_departure_time"] is not None:
            t = this_location["raw_public_departure_time"]
            if last_time is not None:
                if last_time > t:
                    day_incrementor += 1
            d = start_date + timedelta(days=day_incrementor)
            this_location["public_departure_time"] = tz.localize(datetime.combine(d, t)).astimezone(pytz.utc)
        else:
            this_location["public_departure_time"] = None

        if this_location["raw_working_departure_time"] is not None:
            t = this_location["raw_working_departure_time"]
            if last_time is not None:
                if last_time > t:
                    day_incrementor += 1
            d = start_date + timedelta(days=day_incrementor)
            this_location["working_departure_time"] = tz.localize(datetime.combine(d, t)).astimezone(pytz.utc)
        else:
            this_location["working_departure_time"] = None

        # Return the new day_incrementor value.
        return day_incrementor

    def get_last_time(self, l):
        if l["raw_working_departure_time"] is not None:
            return l["raw_working_departure_time"]
        elif l["raw_working_pass_time"] is not None:
            return l["raw_working_pass_time"]
        elif l["raw_working_arrival_time"] is not None:
            return l["raw_working_arrival_time"]
        else:
            raise Exception()


