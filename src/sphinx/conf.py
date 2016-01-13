import sys
import os

sys.path.insert(0, os.path.abspath('_ext'))

extensions = ['includecode']
source_suffix = '.rst'
source_encoding = 'utf-8'
master_doc = 'index'
project = u'Eventuate'
copyright = u'2015 - 2016 Red Bull Media House'
version = '0.3-SNAPSHOT'
release = '0.3-SNAPSHOT'
exclude_patterns = []
highlight_language = 'scala'
html_title = 'Eventuate'
html_theme = 'rbmh'
html_theme_path = ['_themes']
html_theme_options = {
    'project_icon': 'fa-calendar',
    'typekit_id': 'snv5vzo'
}
html_logo = '_themes/rbmh/static/RedBullMediaHouse-dark.png'
html_favicon = '_themes/rbmh/static/favicon.ico'
html_show_sourcelink = False
html_show_sphinx = False

