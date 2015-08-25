from darwindb.postgres.BaseStore import BaseStore
from darwindb.postgres.ScheduleMessageStore import ScheduleMessageStore
from darwindb.utils import subtract_times, apply_date_and_tz_to_time

from datetime import datetime, date, time, timedelta
from dateutil.parser import parse

from collections import OrderedDict

import pytz

class TrainStatusMessageStore(BaseStore):
    
    def __init__(self, connection):
        super().__init__(connection)
        s = ScheduleMessageStore

        self.update_schedule_query = "UPDATE {} SET {} WHERE {}".format(
                s.table_schedule_name,
                ", ".join(["{}=%s".format(k) for k, v in list(s.table_schedule_fields.items())[15:]]),
                "rid=%s")

        self.select_location_query_prepare = "PREPARE ts_select_location_query as SELECT {} from {} WHERE {}".format(
                "id, route_delay, working_arrival_time, public_arrival_time, working_pass_time, public_departure_time, working_departure_time",
                s.table_schedule_location_name,
                " and ".join(["{} IS NOT DISTINCT FROM ${}".format(k, idx+1) for idx, k in enumerate([
                    "rid",
                    "tiploc",
                    "raw_working_arrival_time",
                    #"raw_public_arrival_time",
                    "raw_working_pass_time",
                    #"raw_public_departure_time",
                    "raw_working_departure_time"
                ])]))
        self.select_location_query_execute = "EXECUTE ts_select_location_query (%s, %s, %s, %s, %s)"

        self.select_points_prepare = "PREPARE ts_select_points as SELECT {} from {} WHERE {}".format(
                "id, tiploc, working_arrival_time, public_arrival_time, working_pass_time, public_departure_time, working_departure_time, raw_working_arrival_time, raw_public_arrival_time, raw_working_pass_time, raw_public_departure_time, raw_working_departure_time",
                s.table_schedule_location_name,
                "rid=$1")

        self.update_point_prepare = "PREPARE ts_update_point as UPDATE {} SET {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} WHERE {}".format(
                s.table_schedule_location_name,
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

        self.update_schedule_select_tz_prepare = "PREPARE ts_update_schedule_select_tz as UPDATE {} SET {}, {}, {}, {} WHERE {} RETURNING {}".format(
                s.table_schedule_name,
                "reverse_formation=$1",
                "late_reason_code=$2",
                "late_reason_tiploc=$3",
                "late_reason_near=$4",
                "rid=$5",
                "timezone",)

        self.update_schedule_select_tz_execute = "EXECUTE ts_update_schedule_select_tz (%s, %s, %s, %s, %s)"

        self.cursor = None

    def create_tables(self):
        # No tables to create - the ScheduleMessageStore is responsible for that.
        pass

    def save_train_status_message(self, message):
        # Prepare message
        self.prepare_message(message)

        # Check the train concerned is in the database.
        if self.cursor is None:
            self.cursor = self.connection.cursor()
            self.cursor.execute(self.select_points_prepare)
            self.cursor.execute(self.update_point_prepare)
            self.cursor.execute(self.update_schedule_select_tz_prepare)

        self.cursor.execute("EXECUTE ts_select_points (%s)", (message["rid"],))

        if self.cursor.rowcount == 0:
            print("--- Cannot apply because we don't have the relevant schedule record yet.")
        else:
            print("+++ Schedule record is present. Can apply.")

            # Get the rows from that query.
            rows = self.cursor.fetchall()

            # Get the timezone to apply the schedule to.
            if message.get("late_reason", None) is not None:
                late_reason_code = message["late_reason"].get("code", None)
                late_reason_tiploc = message["late_reason"].get("tiploc", None)
                late_reason_near = message["late_reason"].get("near", None)
            else:
                late_reason_code = None
                late_reason_tiploc = None
                late_reason_near = None

            self.cursor.execute(self.update_schedule_select_tz_execute, (
                message.get("reverse_formation", None),
                late_reason_code,
                late_reason_tiploc,
                late_reason_near,
                message["rid"],
            ))
            if self.cursor.rowcount != 1:
                print("ERROR Row Count not equal to one when getting timezone. This should be impossible.")
                return

            tz = pytz.timezone(self.cursor.fetchall()[0][0])

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

                        self.cursor.execute("EXECUTE ts_update_point (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
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
                if not found:
                    print("    --- Did not find matching row.")
                    pass
            
        self.connection.commit()

    def prepare_message(self, message):

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


