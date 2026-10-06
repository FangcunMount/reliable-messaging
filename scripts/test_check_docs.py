"""Exercise failures the offline documentation gate must actually catch."""
import tempfile
import unittest
from pathlib import Path

from check_docs import anchors, destinations, link_error, without_fences


class DocumentLinks(unittest.TestCase):
    def test_unicode_and_duplicate_anchors(self):
        self.assertEqual(anchors("# 事务与 Outbox\n## 原事务\n## 原事务\n"),
                         {"事务与-outbox", "原事务", "原事务-1"})

    def test_fences_are_ignored_and_must_close(self):
        text, unclosed = without_fences("```python\n[bad](missing.md)\n```\n[ok](README.md)")
        self.assertFalse(unclosed)
        self.assertEqual(destinations(text), ["README.md"])
        self.assertTrue(without_fences("~~~~\nunclosed")[1])

    def test_reference_links_and_inline_code(self):
        self.assertEqual(destinations("[guide][g]\n[g]: guide.md\n`[bad](missing)`"),
                         ["guide.md", "guide.md"])
        self.assertEqual(destinations("[guide][absent]"), ["missing-reference:absent"])

    def test_missing_paths_anchors_and_absolute_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "README.md"
            source.write_text("# 文档\n", encoding="utf-8")
            self.assertIsNone(link_error(root, source, "#文档"))
            self.assertIn("missing anchor", link_error(root, source, "#不存在"))
            self.assertIn("missing target", link_error(root, source, "no.md"))
            self.assertIn("absolute local", link_error(root, source, "/Users/example.md"))
            self.assertIn("leaves repository", link_error(root, source, "../outside.md"))
            self.assertIsNone(link_error(root, source, "https://example.org/release"))


if __name__ == "__main__":
    unittest.main()
