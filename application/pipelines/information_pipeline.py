import pathway as pw


class InformationTransformer:
    def apply(self, value):
        result = value.with_columns(
            is_excluded=pw.apply(
                self.exclude_uid_list,
                value.uid
            )
        )
        return result

    def exclude_uid_list(self, uid: str) -> str:
        return "tttttttt"
