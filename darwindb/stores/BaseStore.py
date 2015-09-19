class StoreMethodNotImplementedError(Exception):
    pass

class BaseStore:

    def save_association_message(self, message, snapshot):
        raise StoreMethodNotImplemented("Store does not implement save_association_message(self, message).")

    def save_deactivated_message(self, message, snapshot):
        raise StoreMethodNotImplemented("Store does not implement save_decativated_message(self, message).")

    def save_schedule_message(self, message, snapshot):
        raise StoreMethodNotImplemented("Store does not implement save_schedule_message(self, message).")

    def save_train_status_message(self, message, snapshot):
        raise StoreMethodNotImplemented("Store does not implement save_train_status_message(self, message).")


