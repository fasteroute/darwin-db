from darwindb.stores import BaseStore
from darwindb.utils import *

from collections import OrderedDict

from datetime import date, datetime, time, timedelta
from dateutil.parser import parse

import psycopg2
import pytz

""" Convenience class that wraps a Postgres Database Connection. """
class Connection:

    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect(self):
        self.conn = psycopg2.connect("host='{}' dbname='{}' user='{}' password='{}'".format(
            self.host, self.dbname, self.user, self.password))

    def cursor(self):
        return self.conn.cursor()
    
    def commit(self):
        return self.conn.commit()


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


class Store(BaseStore):

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

    table_assoc_name = "association"
    table_assoc_fields = OrderedDict([
        ("id", "bigserial PRIMARY KEY NOT NULL"),
        ("tiploc", "varchar NOT NULL"),
        ("category", "varchar NOT NULL"),
        ("deleted", "boolean"),
        ("cancelled", "boolean"),
        ("main_rid", "varchar NOT NULL"),
        ("main_raw_working_arrival_time", "time"),
        ("main_raw_public_arrival_time", "time"),
        ("main_raw_working_pass_time", "time"),
        ("main_raw_public_departure_time", "time"),
        ("main_raw_working_departure_time", "time"),
        ("assoc_rid", "varchar NOT NULL"),
        ("assoc_raw_working_arrival_time", "time"),
        ("assoc_raw_public_arrival_time", "time"),
        ("assoc_raw_working_pass_time", "time"),
        ("assoc_raw_public_departure_time", "time"),
        ("assoc_raw_working_departure_time", "time"),
    ])

    def __init__(self, connection):
        self.connection = connection

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

        self.prepare_insert_assoc_query = "PREPARE assoc_insert AS INSERT into {} ({}) VALUES({})".format(
                self.table_assoc_name,
                ", ".join(["{}".format(k) for k, v in list(self.table_assoc_fields.items())[1:]]),
                ", ".join(["$"+str(n+1) for n in range(0, len(self.table_assoc_fields.items())-1)]),
        )

        self.prepare_update_assoc_query = "PREPARE assoc_update AS UPDATE {} SET {} WHERE {}".format(
                self.table_assoc_name,
                ", ".join(["{}=${}".format(k, i+1) for i, (k, _) in enumerate(list(self.table_assoc_fields.items())[1:])]),
                "id=${}".format(len(self.table_assoc_fields.items()))
        )

        self.execute_insert_assoc_query = "EXECUTE assoc_insert ({})".format(
                ", ".join(["%s" for _ in range(0, len(self.table_assoc_fields.items())-1)])
        )

        self.execute_update_assoc_query = "EXECUTE assoc_update ({})".format(
                ", ".join(["%s" for _ in range(0, len(self.table_assoc_fields.items()))])
        )

        self.prepare_check_assoc_query = "PREPARE assoc_check AS SELECT {} from {} WHERE {}".format(
                "id", self.table_assoc_name, "main_rid=$1 and assoc_rid=$2"
        )

        self.execute_check_assoc_query = "EXECUTE assoc_check (%s, %s)"

        self.select_points_prepare = "PREPARE ts_select_points as SELECT {} from {} WHERE {}".format(
                "id, tiploc, working_arrival_time, public_arrival_time, working_pass_time, public_departure_time, working_departure_time, raw_working_arrival_time, raw_public_arrival_time, raw_working_pass_time, raw_public_departure_time, raw_working_departure_time",
                self.table_schedule_location_name,
                "rid=$1")

        self.update_point_prepare = "PREPARE ts_update_point as UPDATE {} SET {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} WHERE {}".format(
                self.table_schedule_location_name,
                "suppressed=$1",
                "length=$2",
                "detach_front=$3",
                "platform_suppressed=$4",
                "platform_suppressed_by_cis=$5",
                "platform_source=$6",
                "platform_confirmed=$7",
                "platform_number=$8",
                "forecast_arrival_estimated_time=$9",
                "forecast_arrival_working_estimated_time=$10",
                "forecast_arrival_actual_time=$11",
                "forecast_arrival_actual_time_removed=$12",
                "forecast_arrival_manual_estimate_lower_limit=$13",
                "forecast_arrival_manual_estimate_unknown_delay=$14",
                "forecast_arrival_unknown_delay=$15",
                "forecast_arrival_source=$16",
                "forecast_arrival_source_cis=$17",
                "forecast_pass_estimated_time=$18",
                "forecast_pass_working_estimated_time=$19",
                "forecast_pass_actual_time=$20",
                "forecast_pass_actual_time_removed=$21",
                "forecast_pass_manual_estimate_lower_limit=$22",
                "forecast_pass_manual_estimate_unknown_delay=$23",
                "forecast_pass_unknown_delay=$24",
                "forecast_pass_source=$25",
                "forecast_pass_source_cis=$26",
                "forecast_departure_estimated_time=$27",
                "forecast_departure_working_estimated_time=$28",
                "forecast_departure_actual_time=$29",
                "forecast_departure_actual_time_removed=$30",
                "forecast_departure_manual_estimate_lower_limit=$31",
                "forecast_departure_manual_estimate_unknown_delay=$32",
                "forecast_departure_unknown_delay=$33",
                "forecast_departure_source=$34",
                "forecast_departure_source_cis=$35",
                "id=$36")

        self.update_point_arrival_prepare = "PREPARE ts_update_point_arrival as UPDATE {} SET {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} WHERE {}".format(
                self.table_schedule_location_name,
                "suppressed=$1",
                "length=$2",
                "detach_front=$3",
                "platform_suppressed=$4",
                "platform_suppressed_by_cis=$5",
                "platform_source=$6",
                "platform_confirmed=$7",
                "platform_number=$8",
                "forecast_arrival_estimated_time=$9",
                "forecast_arrival_working_estimated_time=$10",
                "forecast_arrival_actual_time=$11",
                "forecast_arrival_actual_time_removed=$12",
                "forecast_arrival_manual_estimate_lower_limit=$13",
                "forecast_arrival_manual_estimate_unknown_delay=$14",
                "forecast_arrival_unknown_delay=$15",
                "forecast_arrival_source=$16",
                "forecast_arrival_source_cis=$17",
                "id=$18")

        self.update_point_departure_prepare = "PREPARE ts_update_point_departure as UPDATE {} SET {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} WHERE {}".format(
                self.table_schedule_location_name,
                "suppressed=$1",
                "length=$2",
                "detach_front=$3",
                "platform_suppressed=$4",
                "platform_suppressed_by_cis=$5",
                "platform_source=$6",
                "platform_confirmed=$7",
                "platform_number=$8",
                "forecast_departure_estimated_time=$9",
                "forecast_departure_working_estimated_time=$10",
                "forecast_departure_actual_time=$11",
                "forecast_departure_actual_time_removed=$12",
                "forecast_departure_manual_estimate_lower_limit=$13",
                "forecast_departure_manual_estimate_unknown_delay=$14",
                "forecast_departure_unknown_delay=$15",
                "forecast_departure_source=$16",
                "forecast_departure_source_cis=$17",
                "id=$18")


        self.update_schedule_select_tz_prepare = "PREPARE ts_update_schedule_select_tz as UPDATE {} SET {}, {}, {}, {} WHERE {} RETURNING {}".format(
                self.table_schedule_name,
                "reverse_formation=$1",
                "late_reason_code=$2",
                "late_reason_tiploc=$3",
                "late_reason_near=$4",
                "rid=$5",
                "timezone",)

        self.update_schedule_select_tz_execute = "EXECUTE ts_update_schedule_select_tz (%s, %s, %s, %s, %s)"



    # Note that this method deliberately doesn't use the @Cursor and @Commit decorators as they rely
    # on the tables already being created for them to work properly.
    def create_tables(self):
        cursor = self.connection.cursor()
        
        schedule_query = "CREATE TABLE IF NOT EXISTS {} ({});".format(
                self.table_schedule_name,
                ", ".join(["{} {}".format(k, v) for k, v in self.table_schedule_fields.items()])
        )
        
        schedule_locations_query = "CREATE TABLE IF NOT EXISTS {} ({});".format(
                self.table_schedule_location_name,
                ", ".join(["{} {}".format(k, v) for k, v in self.table_schedule_location_fields.items()])
        )

        assoc_query = "CREATE TABLE IF NOT EXISTS {} ({})".format(
            self.table_assoc_name,
            ", ".join(["{} {}".format(k, v) for k, v in self.table_assoc_fields.items()])
        )

        # TODO: Create indexes.
        cursor.execute(schedule_query)
        cursor.execute(schedule_locations_query)
        cursor.execute(assoc_query)

        self.connection.commit()
        cursor.close()

    @Commit
    def prepare_queries(self, cursor):
        # Association Queries
        cursor.execute(self.prepare_insert_assoc_query)
        cursor.execute(self.prepare_check_assoc_query)
        cursor.execute(self.prepare_update_assoc_query)

        # Deactivated Queries
        cursor.execute(self.prepare_deactivate_update_query)

        # Schedule Queries
        # TODO: Make all the schedule queries prepared.

        # Train Status Queries
        cursor.execute(self.select_points_prepare)
        cursor.execute(self.update_point_prepare)
        cursor.execute(self.update_point_arrival_prepare)
        cursor.execute(self.update_point_departure_prepare)
        cursor.execute(self.update_schedule_select_tz_prepare)

    @Cursor
    @Commit
    def save_schedule_message(self, message, snapshot=False, cursor=None):
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
        elif cursor.rowcount == 1 and snapshot is False:
            #print("+++ Updating Schedule {}".format(message["rid"]))
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
            cursor.execute("SELECT * from {} WHERE rid=%s".format(self.table_schedule_location_name), (message["rid"],));
            rows = cursor.fetchall()
            #cursor.execute("DELETE from {} WHERE rid=%s".format(self.table_schedule_location_name), (message["rid"],));
            # Loop through each position in the new list of locations.
            updated_rows = []
            position = -1
            for i, p in enumerate(message["locations"]):
                position += 1
                # Loop through the existing locations.
                last_j = None
                for j, r in enumerate(rows):
                    # Continue if we don't have a match.
                    if r[4] != p["tiploc"]:
                        continue
                    if r[10] != p["working_arrival_time"]:
                        continue
                    if r[12] != p["working_pass_time"]:
                        continue
                    if r[14] != p["working_departure_time"]:
                        continue

                    # Looks like we do have a match. Update the database, mark this row as updated
                    # and move on to the next location.
                    cursor.execute("UPDATE {} SET {} WHERE {}".format(
                        self.table_schedule_location_name,
                        "type=%s, "+
                        "position=%s, "+
                        "activity_codes=%s, "+
                        "planned_activity_codes=%s, "+
                        "cancelled=%s, "+
                        "false_tiploc=%s, "+
                        "working_arrival_time=%s, "+
                        "public_arrival_time=%s, "+
                        "working_pass_time=%s, "+
                        "public_departure_time=%s, "+
                        "working_departure_time=%s, "+
                        "raw_working_arrival_time=%s, "+
                        "raw_public_arrival_time=%s, "+
                        "raw_working_pass_time=%s, "+
                        "raw_public_departure_time=%s, "+
                        "raw_working_departure_time=%s",
                        "id=%s"),
                        (   p["location_type"],
                            position,
                            p.get("activity_codes", None),
                            p.get("planned_activity_codes", None),
                            p.get("cancelled", None),
                            p.get("false_tiploc", None),
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
                            r[0]
                        )
                    )

                    last_j = j
                    break
                # Check if we updated a row, or if instead we need to insert a new one.
                if last_j is None:
                    # Need to insert a new one.
                    #print("   +++ Running INSERT on Schedule_Location")
                    cursor.execute(self.insert_schedule_location_query, (
                        message["rid"],
                        p["location_type"],
                        position,
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
             
                else:
                    # We updated one successfully.
                    # Delete from the list of rows the one that we've already updated.
                    del rows[last_j]

            # OK, so now we've updated all the rows we can, and inserted all the new ones, we need
            # to go through all the rows that are left over and delete them.
            for r in rows:
                print("!!! Deleting spurious schedule_location with id {}, rid {}, tiploc {}".format(r[0], r[1], r[4]))
                cursor.execute("DELETE from {} where id=%s".format(self.table_schedule_location_name), (r[0],))
        elif cursor.rowcount == 1 and snapshot is True:
            print("Didn't add schedule {} becuase it's from a snapshot and we arelady have one with that RID.".format(message["rid"]))

       
    @Cursor
    @Commit
    def save_deactivated_message(self, message, snapshot=False, cursor=None):
        rid = message["rid"]
        cursor.execute(self.execute_deactivate_update_query, (rid,));
        if cursor.rowcount != 1:
            print("!!! Could not find a matching schedule to deactivate for RID: {}".format(rid))

    @Cursor
    @Commit
    def save_association_message(self, message, snapshot=False, cursor=None):
        cursor.execute(self.execute_check_assoc_query,
                (message["main_service"]["rid"], message["associated_service"]["rid"])
        )

        if cursor.rowcount == 0:
            #print("+++ Inserting new Association")
            cursor.execute(self.execute_insert_assoc_query, (
                message["tiploc"],
                message["category"],
                message.get("deleted", None),
                message.get("cancelled", None),
                message["main_service"]["rid"],
                message["main_service"].get("working_arrival_time", None),
                message["main_service"].get("public_arrival_time", None),
                message["main_service"].get("working_pass_time", None),
                message["main_service"].get("public_departure_time", None),
                message["main_service"].get("working_departure_time", None),
                message["associated_service"]["rid"],
                message["associated_service"].get("working_arrival_time", None),
                message["associated_service"].get("public_arrival_time", None),
                message["associated_service"].get("working_pass_time", None),
                message["associated_service"].get("public_departure_time", None),
                message["associated_service"].get("working_departure_time", None)
            ))

        elif cursor.rowcount == 1 and snapshot is False:
            #print("+++ Updating existing Association")
            rows = cursor.fetchall()
            aid = rows[0][0]
            cursor.execute(self.execute_update_assoc_query, (
                message["tiploc"],
                message["category"],
                message.get("deleted", None),
                message.get("cancelled", None),
                message["main_service"]["rid"],
                message["main_service"].get("working_arrival_time", None),
                message["main_service"].get("public_arrival_time", None),
                message["main_service"].get("working_pass_time", None),
                message["main_service"].get("public_departure_time", None),
                message["main_service"].get("working_departure_time", None),
                message["associated_service"]["rid"],
                message["associated_service"].get("working_arrival_time", None),
                message["associated_service"].get("public_arrival_time", None),
                message["associated_service"].get("working_pass_time", None),
                message["associated_service"].get("public_departure_time", None),
                message["associated_service"].get("working_departure_time", None),
                aid,
            ))
        elif cursor.rowcount == 1 and snapshot is True:
            print("Didn't add assocation message with rids {} and {} because it's from a snapshot and we already have one in the DB.".format(message["main_service"]["rid"], message["associated_service"]["rid"]))
        else:
            print("$$$ Association message matched {} associations in the database."+\
                  "This Shouldn't happen.".format(cursor.rowcount))
            print("{}".format(message))

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

    @Cursor
    @Commit
    def save_train_status_message(self, message, snapshot=False, cursor=None):
        # Prepare message
        self.prepare_train_status_message(message)

        # Check the train concerned is in the database.
        cursor.execute("EXECUTE ts_select_points (%s)", (message["rid"],))

        if cursor.rowcount == 0:
            print("--- Cannot apply TS because we don't have the relevant schedule record yet. RID: {}".format(message["rid"]))
        else:
            #print("+++ Schedule record is present. Can apply.")

            # Get the rows from that query.
            rows = cursor.fetchall()

            # Get the timezone to apply the schedule to.
            if message.get("late_reason", None) is not None:
                late_reason_code = message["late_reason"].get("code", None)
                late_reason_tiploc = message["late_reason"].get("tiploc", None)
                late_reason_near = message["late_reason"].get("near", None)
            else:
                late_reason_code = None
                late_reason_tiploc = None
                late_reason_near = None

            cursor.execute(self.update_schedule_select_tz_execute, (
                message.get("reverse_formation", None),
                late_reason_code,
                late_reason_tiploc,
                late_reason_near,
                message["rid"],
            ))
            if cursor.rowcount != 1:
                print("ERROR Row Count not equal to one when getting timezone. This should be impossible.")
                return

            tz = pytz.timezone(cursor.fetchall()[0][0])

            for m in message["locations"]:
                found = False
                for r in rows:
                    #print("{} : {} : {} : {}".format(r[1], r[7], r[9], r[11]))
                    if r[1] == m["tiploc"] and \
                    r[7] == m["raw_working_arrival_time"] and \
                    r[9] == m["raw_working_pass_time"] and \
                    r[11] == m["raw_working_departure_time"]:
                        #print("    +++ Found matching row.")
                        found = True
                        
                        # Now figure out the times that belong in the forecasts.
                        # For each time work out if the date has progressed.
                        arrival_working_estimated_time = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("working_estimated_time", None) is not None:
                                arrival_working_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["working_estimated_time"]).time())
                        
                        arrival_estimated_time = None
                        if r[8] is not None:
                            if m["arrival"] is not None and m["arrival"].get("estimated_time", None) is not None:
                                arrival_estimated_time = apply_date_and_tz_to_time(r[3], tz, r[8], parse(m["arrival"]["estimated_time"]).time())
                        elif r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("estimated_time", None) is not None:
                                arrival_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["estimated_time"]).time())
                        
                        arrival_actual_time = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("actual_time", None) is not None:
                                arrival_actual_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["actual_time"]).time())

                        arrival_manual_estimate_lower_limit = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                arrival_manual_estimate_lower_limit = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["manual_estimate_lower_limit_minutes"]).time())

                        arrival_actual_time_removed = None
                        arrival_manual_estimate_unknown_delay = None
                        arrival_unknown_delay = None
                        arrival_source = None
                        arrival_source_cis = None

                        if m.get("arrival", None) is not None:
                            arrival_actual_time_removed = m["arrival"].get("actual_time_removed", None)
                            arrival_manual_estimate_unknown_delay = m["arrival"].get("manual_estimate_unknown_delay", None)
                            arrival_unknown_delay = m["arrival"].get("unknown_delay", None)
                            arrival_source = m["arrival"].get("source", None)
                            arrival_source_cis = m["arrival"].get("source_cis", None)


                        pass_working_estimated_time = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("working_estimated_time", None) is not None:
                                pass_working_estimated_time = apply_date_and_tz_to_time(r[4], tz, r[9], parse(m["pass"]["working_estimated_time"]).time())
                        
                        pass_estimated_time = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("estimated_time", None) is not None:
                                pass_estimated_time = apply_date_and_tz_to_time(r[4], tz, r[9], parse(m["pass"]["estimated_time"]).time())
                        
                        pass_actual_time = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("actual_time", None) is not None:
                                pass_actual_time = apply_date_and_tz_to_time(r[4], tz, r[9], parse(m["pass"]["actual_time"]).time())

                        pass_manual_estimate_lower_limit = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                pass_manual_estimate_lower_limit = apply_date_and_tz_to_time(r[4], tz, r[9], parse(m["pass"]["manual_estimate_lower_limit_minutes"]).time())

                        pass_actual_time_removed = None
                        pass_manual_estimate_unknown_delay = None
                        pass_unknown_delay = None
                        pass_source = None
                        pass_source_cis = None

                        if m.get("pass", None) is not None:
                            pass_actual_time_removed = m["pass"].get("actual_time_removed", None)
                            pass_manual_estimate_unknown_delay = m["pass"].get("manual_estimate_unknown_delay", None)
                            pass_unknown_delay = m["pass"].get("unknown_delay", None)
                            pass_source = m["pass"].get("source", None)
                            pass_source_cis = m["pass"].get("source_cis", None)


                        departure_working_estimated_time = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("working_estimated_time", None) is not None:
                                departure_working_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["working_estimated_time"]).time())
                        
                        departure_estimated_time = None
                        if r[8] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("estimated_time", None) is not None:
                                departure_estimated_time = apply_date_and_tz_to_time(r[3], tz, r[8], parse(m["departure"]["estimated_time"]).time())
                        elif r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("estimated_time", None) is not None:
                                departure_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["estimated_time"]).time())
                        
                        departure_actual_time = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("actual_time", None) is not None:
                                departure_actual_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["actual_time"]).time())

                        departure_manual_estimate_lower_limit = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                departure_manual_estimate_lower_limit = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["manual_estimate_lower_limit_minutes"]).time())

                        departure_actual_time_removed = None
                        departure_manual_estimate_unknown_delay = None
                        departure_unknown_delay = None
                        departure_source = None
                        departure_source_cis = None

                        if m.get("departure", None) is not None:
                            departure_actual_time_removed = m["departure"].get("actual_time_removed", None)
                            departure_manual_estimate_unknown_delay = m["departure"].get("manual_estimate_unknown_delay", None)
                            departure_unknown_delay = m["departure"].get("unknown_delay", None)
                            departure_source = m["departure"].get("source", None)
                            departure_source_cis = m["departure"].get("source_cis", None)

                        
                        if m.get("platform", None) is not None:
                            platform_suppressed = m["platform"].get("suppressed", None)
                            platform_suppressed_by_cis = m["platform"].get("suppressed_by_cis", None)
                            platform_source = m["platform"].get("source", None)
                            platform_confirmed = m["platform"].get("confirmed", None)
                            platform_number = m["platform"].get("number", None)
                        else:
                            platform_suppressed = None
                            platform_suppressed_by_cis = None
                            platform_source = None
                            platform_confirmed = None
                            platform_number = None

                        cursor.execute("EXECUTE ts_update_point (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                            m.get("suppressed", None),
                            m.get("length", None),
                            m.get("detach_front", None),
                            platform_suppressed,
                            platform_suppressed_by_cis,
                            platform_source,
                            platform_confirmed,
                            platform_number,
                            arrival_estimated_time,
                            arrival_working_estimated_time,
                            arrival_actual_time,
                            arrival_actual_time_removed,
                            arrival_manual_estimate_lower_limit,
                            arrival_manual_estimate_unknown_delay,
                            arrival_unknown_delay,
                            arrival_source,
                            arrival_source_cis,
                            pass_estimated_time,
                            pass_working_estimated_time,
                            pass_actual_time,
                            pass_actual_time_removed,
                            pass_manual_estimate_lower_limit,
                            pass_manual_estimate_unknown_delay,
                            pass_unknown_delay,
                            pass_source,
                            pass_source_cis,
                            departure_estimated_time,
                            departure_working_estimated_time,
                            departure_actual_time,
                            departure_actual_time_removed,
                            departure_manual_estimate_lower_limit,
                            departure_manual_estimate_unknown_delay,
                            departure_unknown_delay,
                            departure_source,
                            departure_source_cis,
                            r[0],
                        ))
                        break

                    elif r[1] == m["tiploc"] and \
                    m["raw_working_arrival_time"] is not None and \
                    r[7] == m["raw_working_arrival_time"]:
                        # We have a match based on only arrival time.
                        #print("    +++ Found matching row.")
                        found = True
                        
                        # Now figure out the times that belong in the forecasts.
                        # For each time work out if the date has progressed.
                        arrival_working_estimated_time = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("working_estimated_time", None) is not None:
                                arrival_working_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["working_estimated_time"]).time())
                        
                        arrival_estimated_time = None
                        if r[8] is not None:
                            if m["arrival"] is not None and m["arrival"].get("estimated_time", None) is not None:
                                arrival_estimated_time = apply_date_and_tz_to_time(r[3], tz, r[8], parse(m["arrival"]["estimated_time"]).time())
                        elif r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("estimated_time", None) is not None:
                                arrival_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["estimated_time"]).time())
                        
                        arrival_actual_time = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("actual_time", None) is not None:
                                arrival_actual_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["actual_time"]).time())

                        arrival_manual_estimate_lower_limit = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                arrival_manual_estimate_lower_limit = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["arrival"]["manual_estimate_lower_limit_minutes"]).time())

                        arrival_actual_time_removed = None
                        arrival_manual_estimate_unknown_delay = None
                        arrival_unknown_delay = None
                        arrival_source = None
                        arrival_source_cis = None

                        if m.get("arrival", None) is not None:
                            arrival_actual_time_removed = m["arrival"].get("actual_time_removed", None)
                            arrival_manual_estimate_unknown_delay = m["arrival"].get("manual_estimate_unknown_delay", None)
                            arrival_unknown_delay = m["arrival"].get("unknown_delay", None)
                            arrival_source = m["arrival"].get("source", None)
                            arrival_source_cis = m["arrival"].get("source_cis", None)

                        if m.get("platform", None) is not None:
                            platform_suppressed = m["platform"].get("suppressed", None)
                            platform_suppressed_by_cis = m["platform"].get("suppressed_by_cis", None)
                            platform_source = m["platform"].get("source", None)
                            platform_confirmed = m["platform"].get("confirmed", None)
                            platform_number = m["platform"].get("number", None)
                        else:
                            platform_suppressed = None
                            platform_suppressed_by_cis = None
                            platform_source = None
                            platform_confirmed = None
                            platform_number = None

                        cursor.execute("EXECUTE ts_update_point_arrival (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                            m.get("suppressed", None),
                            m.get("length", None),
                            m.get("detach_front", None),
                            platform_suppressed,
                            platform_suppressed_by_cis,
                            platform_source,
                            platform_confirmed,
                            platform_number,
                            arrival_estimated_time,
                            arrival_working_estimated_time,
                            arrival_actual_time,
                            arrival_actual_time_removed,
                            arrival_manual_estimate_lower_limit,
                            arrival_manual_estimate_unknown_delay,
                            arrival_unknown_delay,
                            arrival_source,
                            arrival_source_cis,
                            r[0],
                        ))
                        break

                    elif r[1] == m["tiploc"] and \
                    m["raw_working_departure_time"] is not None and \
                    r[11] == m["raw_working_departure_time"]:
                        # Got a match based on just the departure time.
                        #print("    +++ Found matching row.")
                        found = True
                        
                        # Now figure out the times that belong in the forecasts.
                        # For each time work out if the date has progressed.
                        departure_working_estimated_time = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("working_estimated_time", None) is not None:
                                departure_working_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["working_estimated_time"]).time())
                        
                        departure_estimated_time = None
                        if r[8] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("estimated_time", None) is not None:
                                departure_estimated_time = apply_date_and_tz_to_time(r[3], tz, r[8], parse(m["departure"]["estimated_time"]).time())
                        elif r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("estimated_time", None) is not None:
                                departure_estimated_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["estimated_time"]).time())
                        
                        departure_actual_time = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("actual_time", None) is not None:
                                departure_actual_time = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["actual_time"]).time())

                        departure_manual_estimate_lower_limit = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                departure_manual_estimate_lower_limit = apply_date_and_tz_to_time(r[2], tz, r[7], parse(m["departure"]["manual_estimate_lower_limit_minutes"]).time())

                        departure_actual_time_removed = None
                        departure_manual_estimate_unknown_delay = None
                        departure_unknown_delay = None
                        departure_source = None
                        departure_source_cis = None

                        if m.get("departure", None) is not None:
                            departure_actual_time_removed = m["departure"].get("actual_time_removed", None)
                            departure_manual_estimate_unknown_delay = m["departure"].get("manual_estimate_unknown_delay", None)
                            departure_unknown_delay = m["departure"].get("unknown_delay", None)
                            departure_source = m["departure"].get("source", None)
                            departure_source_cis = m["departure"].get("source_cis", None)

                        
                        if m.get("platform", None) is not None:
                            platform_suppressed = m["platform"].get("suppressed", None)
                            platform_suppressed_by_cis = m["platform"].get("suppressed_by_cis", None)
                            platform_source = m["platform"].get("source", None)
                            platform_confirmed = m["platform"].get("confirmed", None)
                            platform_number = m["platform"].get("number", None)
                        else:
                            platform_suppressed = None
                            platform_suppressed_by_cis = None
                            platform_source = None
                            platform_confirmed = None
                            platform_number = None

                        cursor.execute("EXECUTE ts_update_point_departure (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                            m.get("suppressed", None),
                            m.get("length", None),
                            m.get("detach_front", None),
                            platform_suppressed,
                            platform_suppressed_by_cis,
                            platform_source,
                            platform_confirmed,
                            platform_number,
                            departure_estimated_time,
                            departure_working_estimated_time,
                            departure_actual_time,
                            departure_actual_time_removed,
                            departure_manual_estimate_lower_limit,
                            departure_manual_estimate_unknown_delay,
                            departure_unknown_delay,
                            departure_source,
                            departure_source_cis,
                            r[0],
                        ))
                        break

                if not found:
                    print("--- Did not find matching schedule_location row for TS {} at {}".format(message["rid"], m["tiploc"]))
                    print("        Times: {} {} {} {} {}".format(
                        m.get("working_arrival_time", None),
                        m.get("public_arrival_time", None),
                        m.get("working_pass_time", None),
                        m.get("public_departure_time", None),
                        m.get("working_departure_time", None)
                    ))
                    pass

    def prepare_train_status_message(self, message):

        for l in message["locations"]:
            if "working_arrival_time" in l:
                l["raw_working_arrival_time"] = parse(l["working_arrival_time"]).time()
            else:
                l["raw_working_arrival_time"] = None

            if "working_pass_time" in l:
                l["raw_working_pass_time"] = parse(l["working_pass_time"]).time()
            else:
                l["raw_working_pass_time"] = None

            if "working_departure_time" in l:
                l["raw_working_departure_time"] = parse(l["working_departure_time"]).time()
            else:
                l["raw_working_departure_time"] = None

            if "public_arrival_time" in l:
                l["raw_public_arrival_time"] = parse(l["public_arrival_time"]).time()
            else:
                l["raw_public_arrival_time"] = None

            if "public_departure_time" in l:
                l["raw_public_departure_time"] = parse(l["public_departure_time"]).time()
            else:
                l["raw_public_departure_time"] = None


