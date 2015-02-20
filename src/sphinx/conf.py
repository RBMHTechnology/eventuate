import sys
import os
import sphinx_rtd_theme

sys.path.insert(0, os.path.abspath('_ext'))

extensions = ['includecode']
source_suffix = '.rst'
source_encoding = 'utf-8'
master_doc = 'index'
project = u'Eventuate'
copyright = u'2015 Red Bull Media House'
version = '0.1-SNAPSHOT'
release = '0.1-SNAPSHOT'
exclude_patterns = []
highlight_language = 'scala'
html_title = 'Eventuate'
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
