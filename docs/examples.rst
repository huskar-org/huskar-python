Examples
========

Client
******

.. code:: python

    from huskar_sdk_v2.http import HttpHuskar

    # Initialize Huskar only with keyword arguments.
    huskar = HttpHuskar(
        app_id='arch.test',
        url='http://api.huskar.example.com',
        token='......',
        soa_mode='route',
        soa_cluster='altb1-channel-stable-1',
    )

``huskar_client`` is fundamental to using nearly any feature of Huskar SDK.

**The following example will assume you already know how to create one.**

-------------------------------------------------------------------------------

Switch
******

.. code:: python

    switch = huskar.switch

    @switch.bind(default='default_data')
    def some_api():
        return 'data'

    # when the switch is ON
    #      switch.is_switched_on('some_api') is True
    #      some_api() == 'data'
    #
    # when the switch is OFF
    #      switch.is_switched_on('some_api') is False
    #      some_api() == 'default_data'

Configuration
**************

.. code:: python

    config = huskar.config
    config.get('some_config')

    @config.on_change('some_config')  # when some_config changes
    def update_config(new_value):
        pass  # do some updating here
