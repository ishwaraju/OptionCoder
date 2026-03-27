import struct
from utils.time_utils import TimeUtils


class BinaryParser:
    """
    Parse Dhan WebSocket Binary Market Feed
    """

    def __init__(self):
        self.time_utils = TimeUtils()

    def parse_header(self, data):
        response_code = data[0]
        message_length = struct.unpack('<H', data[1:3])[0]
        exchange_segment = data[3]
        security_id = struct.unpack('<I', data[4:8])[0]

        return response_code, message_length, exchange_segment, security_id

    def parse_packet(self, data):
        try:
            response_code, message_length, exchange_segment, security_id = self.parse_header(data)

            # =========================
            # TICKER PACKET (Code 2)
            # =========================
            if response_code == 2 and len(data) >= 16:
                ltp = struct.unpack('<f', data[8:12])[0]

                return {
                    "security_id": security_id,
                    "price": round(ltp, 2),
                    "type": "ticker",
                    "time": self.time_utils.current_time_str()
                }

            # =========================
            # OI PACKET (Code 5)
            # =========================
            elif response_code == 5 and len(data) >= 12:
                oi = struct.unpack('<I', data[8:12])[0]

                return {
                    "security_id": security_id,
                    "oi": oi,
                    "type": "oi",
                    "time": self.time_utils.current_time_str()
                }

            # =========================
            # QUOTE PACKET (Code 4)
            # =========================
            elif response_code == 4 and len(data) >= 50:
                ltp = struct.unpack('<f', data[8:12])[0]
                volume = struct.unpack('<I', data[22:26])[0]
                open_price = struct.unpack('<f', data[34:38])[0]
                high = struct.unpack('<f', data[42:46])[0]
                low = struct.unpack('<f', data[46:50])[0]

                return {
                    "security_id": security_id,
                    "price": round(ltp, 2),
                    "volume": volume,
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "type": "quote",
                    "time": self.time_utils.current_time_str()
                }

            # =========================
            # FULL PACKET (Code 8)
            # =========================
            elif response_code == 8 and len(data) >= 62:
                ltp = struct.unpack('<f', data[8:12])[0]
                volume = struct.unpack('<I', data[22:26])[0]
                oi = struct.unpack('<I', data[34:38])[0]
                open_price = struct.unpack('<f', data[46:50])[0]
                high = struct.unpack('<f', data[54:58])[0]
                low = struct.unpack('<f', data[58:62])[0]

                return {
                    "security_id": security_id,
                    "price": round(ltp, 2),
                    "volume": volume,
                    "oi": oi,
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "type": "full",
                    "time": self.time_utils.current_time_str()
                }

            else:
                return None

        except Exception as e:
            print("Binary parse error:", e)
            return None