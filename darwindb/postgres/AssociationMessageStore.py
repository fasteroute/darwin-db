from darwindb.postgres.BaseStore import BaseStore

from collections import OrderedDict

from dateutil.parser import parse

class AssociationMessageStore(BaseStore):

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
        super().__init__(connection)

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

        self.cursor = None

    def create_tables(self):
        cursor = self.connection.cursor()

        assoc_query = "CREATE TABLE IF NOT EXISTS {} ({})".format(
            self.table_assoc_name,
            ", ".join(["{} {}".format(k, v) for k, v in self.table_assoc_fields.items()])
        )

        cursor.execute(assoc_query)

        self.connection.commit()

        cursor.close()

    def save_message(self, message):
        if self.cursor is None:
            self.cursor = self.connection.cursor()
            self.cursor.execute(self.prepare_insert_assoc_query)
            self.cursor.execute(self.prepare_check_assoc_query)
            self.cursor.execute(self.prepare_update_assoc_query)

        self.cursor.execute(self.execute_check_assoc_query,
                (message["main_service"]["rid"], message["associated_service"]["rid"])
        )

        if self.cursor.rowcount == 0:
            #print("+++ Inserting new Association")
            self.cursor.execute(self.execute_insert_assoc_query, (
                message["tiploc"],
                message["category"],
                message.get("deleted", None),
                message.get("cancelled", None),
                message["main_service"].get("rid", None),
                message["main_service"].get("working_arrival_time", None),
                message["main_service"].get("public_arrival_time", None),
                message["main_service"].get("working_pass_time", None),
                message["main_service"].get("public_departure_time", None),
                message["main_service"].get("working_departure_time", None),
                message["associated_service"].get("rid", None),
                message["associated_service"].get("working_arrival_time", None),
                message["associated_service"].get("public_arrival_time", None),
                message["associated_service"].get("working_pass_time", None),
                message["associated_service"].get("public_departure_time", None),
                message["associated_service"].get("working_departure_time", None)
            ))

        else:
            #print("%%% Updating existing Association")
            rows = self.cursor.fetchall()
            aid = rows[0][0]
            self.cursor.execute(self.execute_update_assoc_query, (
                message["tiploc"],
                message["category"],
                message.get("deleted", None),
                message.get("cancelled", None),
                message["main_service"].get("rid", None),
                message["main_service"].get("working_arrival_time", None),
                message["main_service"].get("public_arrival_time", None),
                message["main_service"].get("working_pass_time", None),
                message["main_service"].get("public_departure_time", None),
                message["main_service"].get("working_departure_time", None),
                message["associated_service"].get("rid", None),
                message["associated_service"].get("working_arrival_time", None),
                message["associated_service"].get("public_arrival_time", None),
                message["associated_service"].get("working_pass_time", None),
                message["associated_service"].get("public_departure_time", None),
                message["associated_service"].get("working_departure_time", None),
                aid,
            ))

        self.connection.commit()


