
class CaseStatusException(Exception):
    def __init__(self, message, status_code, case_status):
        super().__init__(message)
        self.status_code = status_code
        self.case_status = case_status