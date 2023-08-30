{% extends "default_message.md" %}

{% block subject %}Vendas BEEXP{% endblock %}

{% block content %}Entrou mais uma venda do BEEXP:
- {{ beexp_order }}
{% endblock %}

{% block updates_info %}{% endblock %}