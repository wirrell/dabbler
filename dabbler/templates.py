"""
Handles template location logic.
"""
import pathlib

CROP_EXPERIMENT_TEMPLATES = {
    'maize': (pathlib.Path(__file__).parent
              / '../data/templates/maize_template.MZX').resolve()
}


def get_template_for_crop(crop):
    with open(CROP_EXPERIMENT_TEMPLATES[crop.lower()], 'r') as f:
        template = f.read()
    return template
