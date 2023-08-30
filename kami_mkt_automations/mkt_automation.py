import logging
import threading as th
import time
from datetime import datetime, timedelta

from constant import ORDER_ITEM_DATE_COLS
from database import get_dataframe_from_sql
from kami_logging import benchmark_with, logging_with
from messages import (
    filter_contact_by_group,
    get_contacts_from_json,
    send_message_by_group,
    send_message_by_seller_id,
)

mkt_automation_logger = logging.getLogger('mkt_autmation')


@benchmark_with(mkt_automation_logger)
@logging_with(mkt_automation_logger)
def get_last_beexp_order() -> datetime:
    return get_dataframe_from_sql('data/in/last_beexp_order.sql')[
        'ultimo_pedido'
    ][0].to_pydatetime()


@benchmark_with(mkt_automation_logger)
@logging_with(mkt_automation_logger)
def get_beexp_since(ordered_date):
    sql_file = 'data/in/beexp_orders.sql'
    oders_df = get_dataframe_from_sql(
        sql_query=sql_file, date_cols=ORDER_ITEM_DATE_COLS
    )
    mask = oders_df['dt_implant'] >= ordered_date
    return oders_df.loc[mask]


@logging_with(mkt_automation_logger)
def get_beexp_manager_message(row):
    (
        index,
        qtd,
        nome_colaborador,
        desc_comercial,
        preco_total,
        cond_pgto,
        cod_cliente,
        nome_cliente,
        dt_implant,
        email_colaborador,
    ) = row

    return f"""Quantidade de Ingressos: {qtd} Nome do Vendedor: {nome_colaborador} Tipo de Ingresso: {desc_comercial} Valor faturado: {preco_total} Forma de pagamento: {cond_pgto} Código do cliente: {cod_cliente} Nome do Cliente: {nome_cliente} Data da venda: {dt_implant} E-mail do representante: {email_colaborador}. O respectivo vendedor já recebeu o link de cadastro do participante."""


@benchmark_with(mkt_automation_logger)
@logging_with(mkt_automation_logger)
def delivery_sellers_message(beexp_orders_df):
    for seller_id in beexp_orders_df['cod_colaborador']:
        send_message_by_seller_id(
            contact_id=seller_id,
            template_name='beexp_manager',
            message_dict={'subject': 'Vendas BEEXP'},
        )


@benchmark_with(mkt_automation_logger)
@logging_with(mkt_automation_logger)
def delivery_managers_message(beexp_orders_df):
    contacts = get_contacts_from_json('messages/contacts.json')
    managers_contacts = filter_contact_by_group(contacts, 'beexp_manager')
    beexp_filtered_df = beexp_orders_df[
        [
            'qtd',
            'nome_colaborador',
            'desc_comercial',
            'preco_total',
            'cond_pgto',
            'cod_cliente',
            'nome_cliente',
            'dt_implant',
            'email_colaborador',
        ]
    ]
    for row in beexp_filtered_df.itertuples():
        beexp_message = get_beexp_manager_message(row)
        send_message_by_group(
            template_name='beexp_manager',
            group='beexp_manager',
            message_dict={
                'subject': 'Vendas BEEXP',
                'beexp_order': beexp_message,
            },
            contacts=managers_contacts,
        )


@logging_with(mkt_automation_logger)
def delivery_messages(beexp_orders_df):
    delivery_sellers_message(beexp_orders_df)
    delivery_managers_message(beexp_orders_df)


def delivery_reports():
    ...


@logging_with(mkt_automation_logger)
def alert_beexp_order():
    first_update = datetime.now() - timedelta(days=1)  # get_last_beexp_order()
    while True:
        current_update = datetime.now()  # get_last_beexp_order()
        if current_update > first_update:
            beexp_orders_df = get_beexp_since(current_update)
            delivery_messages(beexp_orders_df)
        time.sleep(60)


def main_thread():
    while True:
        time.sleep(1)


def run_bot():
    th_update_globals = th.Thread(target=delivery_reports, daemon=True)
    th_main_thread = th.Thread(target=main_thread)
    th_update_globals.start()
    th_main_thread.start()


if __name__ == '__main__':
    run_bot()
