from darwindb.postgres.BaseStore import BaseStore
from darwindb.utils import timezone_for_date_and_time, apply_date_and_tz_to_time, add_minutes_to_time

from collections import OrderedDict

from datetime import datetime, timedelta
from dateutil.parser import parse

import pytz

""" Injects a database cursor into the kwargs of the method call.

The cursor is initialised as a member of the Object, and it's queries are
prepared before the decorated method is actually called.
"""
def Cursor(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not hasattr(self, '_cursor') or self._cursor is None:
            self._cursor = self.connection.cursor()
            self.prepare_queries(self._cursor)
        kwargs["cursor"] = self._cursor
        return f(*args, **kwargs)
    return wrapper

""" Commits current transaction to the database once the decorated method returns.

This decorator uses the object's database connection to commit the transaction
after the decorated method has been completed.
"""
def Commit(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        r = f(*args, **kwargs)
        self.connection.commit()
        return r
    return wrapper

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
            ("timezone", "varchar"),
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
            ("forecast_arrival_estimated_time", "timestamp with time zone"),
            ("forecast_arrival_working_estimated_time", "timestamp with time zone"),
            ("forecast_arrival_actual_time", "timestamp with time zone"),
            ("forecast_arrival_actual_time_removed", "boolean"),
            ("forecast_arrival_manual_estimate_lower_limit", "timestamp with time zone"),
            ("forecast_arrival_manual_estimate_unknown_delay", "boolean"),
            ("forecast_arrival_unknown_delay", "boolean"),
            ("forecast_arrival_source", "varchar"),
            ("forecast_arrival_source_cis", "varchar"),
            ("forecast_departure_estimated_time", "timestamp with time zone"),
            ("forecast_departure_working_estimated_time", "timestamp with time zone"),
            ("forecast_departure_actual_time", "timestamp with time zone"),
            ("forecast_departure_actual_time_removed", "boolean"),
            ("forecast_departure_manual_estimate_lower_limit", "timestamp with time zone"),
            ("forecast_departure_manual_estimate_unknown_delay", "boolean"),
            ("forecast_departure_unknown_delay", "boolean"),
            ("forecast_departure_source", "varchar"),
            ("forecast_departure_source_cis", "varchar"),
            ("forecast_pass_estimated_time", "timestamp with time zone"),
            ("forecast_pass_working_estimated_time", "timestamp with time zone"),
            ("forecast_pass_actual_time", "timestamp with time zone"),
            ("forecast_pass_actual_time_removed", "boolean"),
            ("forecast_pass_manual_estimate_lower_limit", "timestamp with time zone"),
            ("forecast_pass_manual_estimate_unknown_delay", "boolean"),
            ("forecast_pass_unknown_delay", "boolean"),
            ("forecast_pass_source", "varchar"),
            ("forecast_pass_source_cis", "varchar"),
    ])

    def __init__(self, connection):
        super().__init__(connection)

        self.insert_schedule_query = "INSERT into {} ({}) VALUES({})".format(
                self.table_schedule_name,
                ", ".join(["{}".format(k) for k, v in list(self.table_schedule_fields.items())[0:15]]),
                ", ".join(["%s" for n in range(0, 15)]))
        
        self.insert_schedule_location_query = "INSERT into {} ({}) VALUES({})".format(
                self.table_schedule_location_name,
                ", ".join(["{}".format(k) for k, v in list(self.table_schedule_location_fields.items())[1:20]]),
                ", ".join(["%s" for n in range(0, 19)]))

        self.update_schedule_query = "UPDATE {} SET {} WHERE {}".format(
                self.table_schedule_name,
                ", ".join(["{}=%s".format(k) for k, v in list(self.table_schedule_fields.items())[1:15]]),
                "rid=%s")

        self.prepare_deactivate_update_query = "PREPARE de_update AS UPDATE {} SET {} WHERE {}".format(
                self.table_schedule_name,
                "active=false",
                "rid=$1"
        )

        self.execute_deactivate_update_query = "EXECUTE de_update (%s)"

    # Note that this method deliberately doesn't use the @Cursor and @Commit decorators as they rely
    # on the tables already being created for them to work properly.
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

    @Commit
    def prepare_queries(self, cursor):
        cursor.execute(self.prepare_deactivate_update_query)

    @Cursor
    @Commit
    def save_schedule_message(self, message, cursor):
        # Calculate the date-times on the schedule message.
        self.build_sanitised_times(message)

        # Check if the schedule message is already found with that RTTI ID
        cursor.execute("SELECT rid FROM schedule WHERE rid=%s", (message["rid"],));

        #print("*** Saving Schedule Message.")

        # Check if this is a new Schedule.
        if cursor.rowcount == 0:
            cursor.execute(self.insert_schedule_query, (
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
                message["timezone"].zone,
            ))
        else:
           cursor.execute(self.update_schedule_query, (
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
                message["timezone"].zone,
                message["rid"],
            ))
           # FIXME: Save forecast components before deleting...
           cursor.execute("DELETE from {} WHERE rid=%s".format(self.table_schedule_location_name), (message["rid"],));
        i = 0
        for p in message["locations"]:
            cursor.execute(self.insert_schedule_location_query, (
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
        
    @Cursor
    @Commit
    def save_deactivated_message(self, message, cursor):
        rid = message["rid"]

        cursor.execute(self.execute_deactivate_update_query, (rid,));

        if cursor.rowcount != 1:
            print("!!! Could not find a matching schedule to deactivate for RID: {}".format(rid))

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

            l["working_arrival_time"] = None
            l["public_arrival_time"] = None
            l["working_pass_time"] = None
            l["public_departure_time"] = None
            l["working_departure_time"] = None

        # Get the first time to use to figure out the timezone of the schedule.
        first_location = message["locations"][0]
        if first_location.get("raw_working_arrival_time", None) is not None:
            t = first_location["raw_working_arrival_time"]
        elif first_location.get("raw_public_arrival_time", None) is not None:
            t = first_location["raw_public_arrival_time"]
        elif first_location.get("raw_working_pass_time", None) is not None:
            t = first_location["raw_working_pass_time"]
        elif first_location.get("raw_public_departure_time", None) is not None:
            t = first_location["raw_public_departure_time"]
        elif first_location.get("raw_working_departure_time", None) is not None:
            t = first_location["raw_working_departure_time"]
        else:
            raise Exception()

        message["timezone"] = timezone_for_date_and_time(message["start_date"], t)

        previous_location = None
        for this_location in message["locations"]:
            self.build_times(previous_location, this_location, message["start_date"], message["timezone"])
            previous_location = this_location

    def build_times(self, previous_location, this_location, start_date, tz):

        if this_location["raw_working_arrival_time"] is not None:
            this_time = this_location["raw_working_arrival_time"]
            previous_time = self.get_last_time(previous_location, this_location, start_date, tz)
            this_location["working_arrival_time"] = \
                    apply_date_and_tz_to_time(previous_time, tz, previous_time.time(), this_time)
        else:
            this_location["working_arrival_time"] = None

        if this_location["raw_public_arrival_time"] is not None:
            this_time = this_location["raw_public_arrival_time"]
            previous_time = self.get_last_time(previous_location, this_location, start_date, tz)
            this_location["public_arrival_time"] = \
                    apply_date_and_tz_to_time(previous_time, tz, previous_time.time(), this_time)
        else:
            this_location["public_arrival_time"] = None

        if this_location["raw_working_pass_time"] is not None:
            this_time = this_location["raw_working_pass_time"]
            previous_time = self.get_last_time(previous_location, this_location, start_date, tz)
            this_location["working_pass_time"] = \
                    apply_date_and_tz_to_time(previous_time, tz, previous_time.time(), this_time)
        else:
            this_location["working_pass_time"] = None

        if this_location["raw_public_departure_time"] is not None:
            this_time = this_location["raw_public_departure_time"]
            previous_time = self.get_last_time(previous_location, this_location, start_date, tz)
            this_location["public_departure_time"] = \
                    apply_date_and_tz_to_time(previous_time, tz, previous_time.time(), this_time)
        else:
            this_location["public_departure_time"] = None

        if this_location["raw_working_departure_time"] is not None:
            this_time = this_location["raw_working_departure_time"]
            previous_time = self.get_last_time(previous_location, this_location, start_date, tz)
            this_location["working_departure_time"] = \
                    apply_date_and_tz_to_time(previous_time, tz, previous_time.time(), this_time)
        else:
            this_location["working_departure_time"] = None

    def get_last_time(self, previous_location, this_location, start_date, tz):
        if this_location["working_departure_time"] is not None:
            return this_location["working_departure_time"]
        if this_location["public_departure_time"] is not None:
            return this_location["public_departure_time"]
        if this_location["working_pass_time"] is not None:
            return this_location["working_pass_time"]
        if this_location["public_arrival_time"] is not None:
            return this_location["public_arrival_time"]
        if this_location["working_arrival_time"] is not None:
            return this_location["working_arrival_time"]

        if previous_location is None:
            t = None
            if this_location["raw_working_arrival_time"] is not None:
                t = this_location["raw_working_arrival_time"]
            if this_location["raw_public_arrival_time"] is not None:
                t = this_location["raw_public_arrival_time"]
            if this_location["raw_working_pass_time"] is not None:
                t = this_location["raw_working_pass_time"]
            if this_location["raw_public_departure_time"] is not None:
                t = this_location["raw_public_departure_time"]
            if this_location["raw_working_departure_time"] is not None:
                t = this_location["raw_working_departure_time"]

            if t is None:
                raise Exception("T is none and previous_location is None. WTF?")

            t = add_minutes_to_time(t, this_location.get("route_delay", None))

            return tz.localize(datetime.combine(start_date, t)).astimezone(pytz.utc)
        
        if previous_location["working_departure_time"] is not None:
            return previous_location["working_departure_time"]
        if previous_location["public_departure_time"] is not None:
            return previous_location["public_departure_time"]
        if previous_location["working_pass_time"] is not None:
            return previous_location["working_pass_time"]
        if previous_location["public_arrival_time"] is not None:
            return previous_location["public_arrival_time"]
        if previous_location["working_arrival_time"] is not None:
            return this_location["working_arrival_time"]

        raise Exception("Couldn't figure out the last time.")


