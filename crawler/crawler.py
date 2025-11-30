class BaseCrawler:
    """
    Base interface for all regulator crawlers.
    Every crawler (SBP, SECP, etc.) must implement these two methods.
    """

    def get_structure(self):
        """
        Returns the folder structure of the regulator's regulatory section.

        Expected return (example):
        [
            {"name": "Circulars", "url": "...", "parent": None},
            {"name": "2025", "url": "...", "parent": "Circulars"},
        ]
        """
        raise NotImplementedError("get_structure() must be implemented")

    def get_documents(self):
        """
        Returns list of regulatory documents.

        Expected return (example):
        [
            {"title": "Circular 3 of 2025", "url": "...", "folder_url": "..."}
        ]
        """
        raise NotImplementedError("get_documents() must be implemented")
