import ecs_logging

class FasalStdlibFormatter(ecs_logging.StdlibFormatter):
    def __init__(self, datefmt=None, extra=None, exclude_fields=()):

        # Keep required keys and default values
        self._required = {
            'severity': '',
            'source': '',
            'env': '',
            'type': 'app-log',
            'message': '',
            'label': ''
        }

        # Rename any key with custom field
        # nested dict can be renamed with dot operator
        self._rename_field = {
            'severity': 'log.level',
        }

        # Copy a filed value without deleting
        self._copy_field = {
            "source": 'log.origin.file.name'
        }

        if extra is not None:
            self._required.update(extra)
        super().__init__(
            datefmt = datefmt,
            exclude_fields = exclude_fields,
            extra = extra
        )
    
    def _rename(self, source, ref_dict, keep_keys, drop=True):
        for key in self._required.keys():
            if key in ref_dict.keys():
                try:
                    source[key] = source[ref_dict.get(key)]
                    if drop: del source[ref_dict.get(key)]
                except KeyError:
                    pass

            # If key already exists, remove the default value
            if key in source.keys():
                try:
                    keep_keys.remove(key)
                except ValueError:
                    pass
        return keep_keys, source

    def format_to_ecs(self, record):
        result = super().format_to_ecs(record)
        result = ecs_logging._utils.flatten_dict(result)

        # rename the result
        default_keys = list(self._required.keys())
        default_keys, result = self._rename(result, ref_dict=self._rename_field, keep_keys=default_keys, drop=True)

        # Copy fields from result
        default_keys, result = self._rename(result, ref_dict=self._copy_field, keep_keys=default_keys, drop=False)

        result.update({key: self._required[key] for key in default_keys})
        return ecs_logging._utils.normalize_dict(result)
