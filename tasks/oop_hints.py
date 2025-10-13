class Room:
    __slots__ = ["r_number", "r_type", "reserved", "guest_name"]

    r_number: int
    r_type: str
    reserved: bool
    guest_name: str | None

    def __init__(self, r_number: int, r_type: str, reserved: bool = True, guest_name: str | None = None) -> None:
        self.r_number = r_number
        self.r_type = r_type
        self.reserved = reserved
        self.guest_name = guest_name

    def __str__(self) -> str:
        return f"{self.r_number, self.r_type, self.reserved, self.guest_name}"

    def __repr__(self) -> str:
        return f"{self.r_number, self.r_type, self.reserved, self.guest_name}"
      
