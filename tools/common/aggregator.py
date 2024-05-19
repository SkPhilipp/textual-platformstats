class BaseAggregatorConfig:
    def __init__(self, filter_self=True):
        self.filter_self = filter_self


class BaseAggregator[T: BaseAggregatorConfig]:
    def __init__(self, config: T):
        self.config = config
        self.data = {}

    def counts_for_key(self, key):
        """
        Count the number of occurrences of each distinct value for a given key in the data.

        :return:
        """
        counts = {}
        for item in self.data[key]:
            counts[item] = counts.get(item, 0) + 1
        return counts

    def summarize_counts_for_key(self, key, top_n=20):
        """
        Summarize the data for a given key.

        :param key:
        :param top_n:
        :return:
        """
        counts = self.counts_for_key(key)
        total = sum(counts.values())
        print(f"Unique {key}: (top {top_n}) (total: {total})")
        for count_item, counts in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:top_n]:
            print(f"- \"{count_item}\" {counts}")
