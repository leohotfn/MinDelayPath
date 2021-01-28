# coding:utf-8

import unittest

from mindelaypath import MinDelayPathController


class TestMinDelayPathController(unittest.TestCase):
    def test_init(self):
        ctrler = MinDelayPathController()
        self.assertIsNotNone(ctrler)

    def test_get_paths(self):
        test_cases_list = [
            # 这里忽略相邻的两两交换机之间的端口，因此设置为 0
            {
                "src": 1,
                "dst": 4,
                "input": {
                    1: {2: 0, 3: 0},
                    2: {1: 0, 4: 0}, 3: {1: 0, 4: 0},
                    4: {2: 0, 3: 0}
                },
                "expect": [[1, 2, 4], [1, 3, 4]]
            },
            {
                "src": 1,
                "dst": 5,
                "input": {
                    1: {2: 0, 3: 0},
                    2: {1: 0, 5: 0}, 3: {1: 0, 4: 0}, 4: {3: 0, 5: 0},
                    5: {2: 0, 4: 0}
                },
                "expect": [[1, 2, 5], [1, 3, 4, 5]]
            },
            {
                "src": 1,
                "dst": 5,
                "input": {
                    1: {2: 0, 3: 0, 4: 0},
                    2: {1: 0, 5: 0}, 3: {1: 0, 5: 0}, 4: {1: 0, 5: 0},
                    5: {2: 0, 3: 0, 4: 0}
                },
                "expect": [[1, 2, 5], [1, 3, 5], [1, 4, 5]]
            },
            {
                "src": 1,
                "dst": 8,
                "input": {
                    1: {2: 0, 3: 0, 4: 0},
                    2: {1: 0, 5: 0}, 3: {1: 0, 6: 0}, 4: {1: 0, 8: 0},
                    5: {2: 0, 7: 0}, 7: {5: 0, 8: 0}, 6: {3: 0, 8: 0},
                    8: {4: 0, 6: 0, 7: 0}
                },
                "expect": [[1, 2, 5, 7, 8], [1, 3, 6, 8], [1, 4, 8]]
            },
            {
                "src": 1,
                "dst": 5,
                "input": {
                    1: {2: 0, 3: 0, 6: 0, 9: 0},
                    2: {1: 0, 5: 0}, 3: {1: 0, 4: 0}, 6: {1: 0, 7: 0}, 9: {1: 0, 10: 0},
                    4: {3: 0, 5: 0}, 7: {6: 0, 8: 0}, 10: {9: 0, 11: 0},
                    8: {7: 0, 5: 0}, 11: {10: 0, 5: 0},
                    5: {2: 0, 4: 0, 8: 0, 11: 0}
                },
                "expect": [[1, 9, 10, 11, 5], [1, 2, 5], [1, 3, 4, 5], [1, 6, 7, 8, 5]]
            },
        ]

        ctr = MinDelayPathController()
        for case in test_cases_list:
            ctr.switch_link_dict = case["input"]
            actual_lst = ctr.get_paths(case["src"], case["dst"])
            expect_list = case["expect"]
            self.assertListEqual(sorted(actual_lst), sorted(expect_list))

    def test_get_link_delay(self):
        test_delay_dict = {
            1: {
                2: 50,
                3: 80,
            },
            2: {
                1: 80,
                4: 15,
            },
            3: {
                1: 20,
                4: 50,
            },
            4: {
                2: 100,
                3: 230,
            }
        }

        test_cases_list = [
            {
                "s1": 1,
                "s2": 2,
                "expect": (test_delay_dict[1][2] + test_delay_dict[2][1]) / 2
            },
            {
                "s1": 1,
                "s2": 3,
                "expect": (test_delay_dict[1][3] + test_delay_dict[3][1]) / 2
            },
            {
                "s1": 2,
                "s2": 4,
                "expect": (test_delay_dict[2][4] + test_delay_dict[4][2]) / 2
            },
            {
                "s1": 1,
                "s2": 4,
                "expect": float("inf")
            },
        ]

        ctr = MinDelayPathController()
        ctr.link_delay_dict = test_delay_dict

        for case in test_cases_list:
            actcual = ctr.get_link_delay(case["s1"], case["s2"])
            expect = case["expect"]
            self.assertEqual(actcual, expect)

    def test_get_path_delay(self):
        test_delay_dict = {
            1: {
                2: 50,
                3: 80,
            },
            2: {
                1: 80,
                4: 15,
            },
            3: {
                1: 20,
                4: 50,
            },
            4: {
                2: 100,
                3: 230,
            }
        }

        test_cases_list = [
            {
                "path": [1, 2],
                "expect": 65,
            },
            {
                "path": [1, 3],
                "expect": 50,
            },
            {
                "path": [1, 2, 4],
                "expect": 122,
            },
            {
                "path": [1, 3, 4],
                "expect": 190,
            },
        ]

        ctr = MinDelayPathController()
        ctr.link_delay_dict = test_delay_dict

        for case in test_cases_list:
            actual = ctr.get_path_delay(case["path"])
            self.assertEqual(actual, case["expect"])

    def test_add_ports_to_paths(self):
        DEFAULT_FRIST_PORT = 0
        DEFAULT_LAST_PORT = 10000
        test_cases_list = [
            {
                "switch_link": {
                    1: {2: 2},
                    2: {1: 1},
                },
                "paths": [
                    [1, 2],
                ],
                "first_port": DEFAULT_FRIST_PORT,
                "last_port": DEFAULT_LAST_PORT,
                "expect": [
                    {1: (DEFAULT_FRIST_PORT, 2), 2: (1, DEFAULT_LAST_PORT)}
                ]
            },
            {
                "switch_link": {
                    1: {2: 2, 3: 3},
                    2: {1: 1, 4: 4}, 3: {1: 1, 4: 4},
                    4: {2: 2, 3: 3}
                },
                "paths": [
                    [1, 2, 4],
                    [1, 3, 4]
                ],
                "first_port": DEFAULT_FRIST_PORT,
                "last_port": DEFAULT_LAST_PORT,
                "expect": [
                    {1: (DEFAULT_FRIST_PORT, 2), 2: (1, 4), 4: (2, DEFAULT_LAST_PORT)},
                    {1: (DEFAULT_FRIST_PORT, 3), 3: (1, 4), 4: (3, DEFAULT_LAST_PORT)},
                ]
            },
            {
                "switch_link": {
                    1: {2: 2, 3: 3, 4: 4},
                    2: {1: 1, 5: 5}, 3: {1: 1, 6: 6}, 4: {1: 1, 8: 8},
                    5: {2: 2, 7: 7}, 7: {5: 5, 8: 8}, 6: {3: 3, 8: 8},
                    8: {4: 4, 6: 6, 7: 7}
                },
                "paths": [
                    [1, 2, 5, 7, 8],
                    [1, 3, 6, 8],
                    [1, 4, 8]
                ],
                "first_port": DEFAULT_FRIST_PORT,
                "last_port": DEFAULT_LAST_PORT,
                "expect": [
                    {1: (DEFAULT_FRIST_PORT, 2), 2: (1, 5), 5: (2, 7), 7: (5, 8), 8: (7, DEFAULT_LAST_PORT)},
                    {1: (DEFAULT_FRIST_PORT, 3), 3: (1, 6), 6: (3, 8), 8: (6, DEFAULT_LAST_PORT)},
                    {1: (DEFAULT_FRIST_PORT, 4), 4: (1, 8), 8: (4, DEFAULT_LAST_PORT)},
                ]
            }
        ]

        ctr = MinDelayPathController()
        for case in test_cases_list:
            ctr.switch_link_dict = case["switch_link"]
            actual = ctr.add_ports_to_paths(case["paths"], case["first_port"], case["last_port"])
            self.assertListEqual(actual, case["expect"])


if __name__ == "__main__":
    unittest.main()