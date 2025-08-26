from hashlib import sha256
import plotly.figure_factory as ff
from plotly.offline import get_plotlyjs
from jinja2 import Template


def test_plotly_html_figure():
    template = """
    <html>
        <head>
            <script type="text/javascript">{{ plotlyjs }}</script>
        </head>
        <body>
        {%- for figure in figures %}
            {{ figure }}
        {%- endfor -%}
        </body>
    </html>
    """
    fig = ff.create_table([["a", "b"], [1, 2]])
    div1 = fig.to_html(full_html=False, include_plotlyjs=False)
    div2 = fig.to_html(full_html=False, include_plotlyjs=False)
    j2_template = Template(template)
    j2_template.render({"figures": [div1, div2], "plotlyjs": get_plotlyjs()})