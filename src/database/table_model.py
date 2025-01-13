class TableModel:
    def __init__(self, col_name: str, col_type: str, nullable: bool):
        self.col_name = col_name
        self.col_type = col_type
        self.nullable = nullable