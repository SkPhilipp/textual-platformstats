from typing import Tuple


class Gradient:
    def __init__(self, start_rgb: Tuple[int, int, int], end_rgb: Tuple[int, int, int], num_colors=100):
        self.start_rgb = start_rgb
        self.end_rgb = end_rgb
        self.num_colors = num_colors

    def __len__(self):
        return self.num_colors

    def __getitem__(self, item):
        r = int(self.start_rgb[0] + (self.end_rgb[0] - self.start_rgb[0]) * item / self.num_colors)
        g = int(self.start_rgb[1] + (self.end_rgb[1] - self.start_rgb[1]) * item / self.num_colors)
        b = int(self.start_rgb[2] + (self.end_rgb[2] - self.start_rgb[2]) * item / self.num_colors)
        a_clamped = max(0, min(255, r))
        b_clamped = max(0, min(255, g))
        c_clamped = max(0, min(255, b))
        return f"rgb({a_clamped},{b_clamped},{c_clamped})"


GradientBlackWhite = Gradient((0, 0, 0), (255, 255, 255))
GradientRedGreen = Gradient((255, 0, 0), (0, 255, 0))
GradientPurpleYellow = Gradient((128, 0, 128), (255, 255, 0))
