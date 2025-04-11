
class CommandInterface:
    @classmethod
    def name(cls):
        raise NotImplementedError("Subclasses should implement this method.")

    @classmethod
    def register(cls, subparsers):
        pass

    @classmethod
    def execute(cls, args):
        raise NotImplementedError("Subclasses should implement this method.")