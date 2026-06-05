class ModelRouter:

    def route(self, player_position: str) -> str:

        pos = player_position.upper()

        if "GK" in pos:
            return "GK"

        if "FW" in pos:
            return "FW"

        if "DF" in pos:
            return "DF"

        if "MF" in pos:
            return "MF"

        return "MF"