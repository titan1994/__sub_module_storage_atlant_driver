from MODS.rest_core.pack_core.RUN.PRE_LAUNCH import APP_INIT

from MODS.DRIVERS.kafka_proc.driver import KafkaProducerConfluent

with KafkaProducerConfluent(
        use_tx=False,
        one_topic_name='____sub_module_storage_atlant_showcase_kafka_queue_basicclient_passportkfh'
) as kp:
    for i in range(37):
        kp.put_data(
            key='test1',
            value={
                'INN': f'99777_{i}',
                'Size': 7000 + i
            }
        )
