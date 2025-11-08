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


class HotelBookingService:

    rooms: list[Room]

    def __init__(self) -> None:
        self.rooms = [Room(101, "single"),
                      Room(102, "double"),
                      Room(103, "suite")]

    def book_room(self, desired_type: str, guest_name: str) -> Room | None:
        available_rooms = list(filter(lambda room: room.r_type == desired_type, self.rooms))
        room_to_book = next((room for room in available_rooms if not room.reserved), None)
        if not room_to_book:
            return None
        room_to_book.reserved = True
        room_to_book.guest_name = guest_name
        return room_to_book

    def cancel_booking(self, room_number: int) -> Room | None:
        room_to_cancel = next((room for room in self.rooms if room.r_number == room_number), None)
        if not room_to_cancel:
            return None
        room_to_cancel.reserved = False
        room_to_cancel.guest_name = None
        return room_to_cancel

    def list_available_rooms(self) -> list[Room | None]:
        return list(filter(lambda room: not room.reserved, self.rooms))

    def get_room_details(self, room_number: int) -> Room | None:
        return next((room for room in self.rooms if room.r_number == room_number), None)

    def list_guests(self) -> list[str | None]:
        return list(map(lambda room: room.guest_name, filter(lambda room: room.reserved, self.rooms)))

    def change_room_type(self, room_number: int, new_type: str) -> bool:
        room_to_change = next((room for room in self.rooms if room.r_number == room_number), None)
        if not room_to_change:
            return False
        room_to_change.r_type = new_type
        return True

    def get_total_rooms_by_type(self, room_type: str) -> int:
        return len(list(filter(lambda room: room.r_type == room_type, self.rooms)))

    def get_reserved_rooms_by_type(self, room_type: str) -> int:
        return len(list(filter(lambda room: room.r_type == room_type and room.reserved, self.rooms)))


if __name__ == "__main__":
    hotel = HotelBookingService()
