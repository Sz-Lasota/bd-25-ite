from typing import Any


class Table:

    def __init__(
        self, name: str, attributes: list[str], entities: list[tuple[Any]]
    ) -> None:
        self.name = name
        self.attributes = attributes
        self.entities = self._transform_to_dict(entities)

    def _transform_to_dict(self, entities: list[tuple[Any]]) -> list[dict[str, Any]]:
        rows = []
        for row in entities:
            rows.append({k: v for k, v in zip(self.attributes, row)})

        return rows

    def __repr__(self) -> str:
        col_width = len(max(self.attributes, key=lambda it: len(it)))

        for row in self.entities:
            col_width = (
                len(str(new_max))
                if len(str((new_max := max(row.values(), key=lambda it: len(str(it))))))
                > col_width
                else col_width
            )

        repr = [" | ".join(map(lambda it: f"{str(it):^{col_width}}", self.attributes))]
        repr.append("-" * len(repr[-1]))
        repr.extend(
            [
                " | ".join(map(lambda it: f"{str(it):<{col_width}}", row.values()))
                for row in self.entities
            ]
        )

        return "\n".join(repr)
