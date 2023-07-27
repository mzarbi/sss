from collections import defaultdict


class Node:
    def __init__(self):
        self.children = defaultdict(Node)
        self.value = None


class Trie:
    def __init__(self):
        self.root = Node()

    def insert(self, keys, value):
        node = self.root
        for key in keys:
            node = node.children[key]
        node.value = value

    def search(self, keys):
        node = self.root
        for key in keys:
            node = node.children.get(key)
            if node is None:
                return None
        return node.value

    def _traverse(self, node, path, all_paths):
        if node.value is not None:
            all_paths.append(path)
        for key, child in node.children.items():
            self._traverse(child, path + [key], all_paths)

    def keys(self):
        all_paths = []
        self._traverse(self.root, [], all_paths)
        return all_paths