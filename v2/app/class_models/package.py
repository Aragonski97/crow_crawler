from .selector import CrowSelector, SelectorList


class Package:

    def __init__(
            self,
            pipeline_name: str,
            step_order_id: int,
            data: list,
            selectors: list[CrowSelector] | None = None,
            closed_inbound: bool = False
    ) -> None:
        self.pipeline_name = pipeline_name
        self.step_order_id = step_order_id
        self.data = data
        self.selectors = SelectorList(selectors).split_selectors() if selectors is not None else None
        # closed_inbound refers to final item coming from a particular step
        self.closed_inbound = closed_inbound
