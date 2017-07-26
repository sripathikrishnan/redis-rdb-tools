
class IOWrapper(object):
    def __init__(self, io_object):
        self.io_object = io_object
        self.record_buffer = False
        self.record_buffer_size = False
        self.bytes = bytes()
        self.buffer_size = 0

    def start_recording(self):
        self.record_buffer = True

    def start_recording_size(self):
        self.record_buffer_size = True

    def get_recorded_buffer(self):
        return self.bytes

    def get_recorded_size(self):
        return self.buffer_size

    def stop_recording(self):
        self.record_buffer = False
        self.bytes = bytes()

    def stop_recording_size(self):
        self.record_buffer_size = True
        self.buffer_size = 0

    def read(self, n_bytes):
        current_bytes = self.io_object.read(n_bytes)

        if self.record_buffer:
            self.bytes += current_bytes

        if self.record_buffer_size:
            self.buffer_size += len(current_bytes)

        return current_bytes
