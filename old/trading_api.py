import asyncio
import ccxt.async_support as ccxt
from aiolimiter import AsyncLimiter
from loguru import logger
from ccxt.base.errors import OrderNotFound

import helpers


class TradingAPI:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.init_limiter()

    def init_limiter(self):
        self.limiter = AsyncLimiter(10, 1)

    async def bound_fetch(self, bound_task: str, inner_metadata: dict):
        async with self.limiter:
            if bound_task == "fetch_orders":
                symbol = inner_metadata["symbol"]
                try:
                    response = await self.exchange.fetch_orders(
                        symbol=symbol,
                        params={'limit': 100}  # You may need to adjust the limit based on your needs
                    )
                except Exception as e:
                    return {"response": e, "symbol": symbol}
                return {"response": response, "symbol": symbol}
            elif bound_task == "cancel_multi_orders":
                order_id = inner_metadata["order_id"]
                symbol = inner_metadata["symbol"]
                position_table_id = inner_metadata["position_table_id"]
                trader_id = inner_metadata["trader_id"]
                db_position_roe = inner_metadata["roe"]
                try:
                    order = await self.exchange.cancel_order(
                        id=order_id,
                        symbol=symbol,
                    )
                    order["position_table_id"] = position_table_id
                    order["is_ignored"] = inner_metadata["is_ignored"]
                    order["is_ignored_reason"] = inner_metadata["is_ignored_reason"]
                    order["trader_id"] = trader_id
                    order["db_position_roe"] = db_position_roe
                    success_msg = f'Order successfully canceled: ({bound_task}), ({symbol}), ({position_table_id})'
                    logger.debug(success_msg)
                    return True, order
                except Exception as e:
                    error_msg = f'Failed to cancel: ({bound_task}), ({symbol}), ({position_table_id}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "close_multi_orders":
                symbol = inner_metadata["symbol"]
                opossite_side = self.flip_side(side=inner_metadata["side"])
                quantity_to_close = inner_metadata["user_amount"]
                order_id = inner_metadata["order_id"]
                position_table_id = inner_metadata["position_table_id"]
                trader_id = inner_metadata["trader_id"]
                db_position_roe = inner_metadata["roe"]
                try:
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type='market',
                        side=opossite_side,
                        amount=quantity_to_close,
                        params={'reduceOnly': True, 'orderId': order_id}
                    )
                    order["user_amount"] = inner_metadata["user_amount"]
                    order["position_table_id"] = position_table_id
                    order["trader_id"] = trader_id
                    order["db_position_roe"] = db_position_roe
                    success_msg = f'Order successfully closed: ({bound_task}), ({symbol}), ({position_table_id})'
                    logger.debug(success_msg)
                    return True, order
                except Exception as e:
                    error_msg = f'Failed to close: ({bound_task}), ({symbol}), ({position_table_id}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "partially_close_multi_orders":
                symbol = inner_metadata["symbol"]
                opossite_side = self.flip_side(side=inner_metadata["side"])
                amount_original = inner_metadata["amount_original"]
                quantity_to_close = inner_metadata["quantity_to_close"]
                order_id = inner_metadata["order_id"]
                position_table_id = inner_metadata["position_table_id"]
                try:
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type='market',
                        side=opossite_side,
                        amount=quantity_to_close,
                        params={'reduceOnly': True, 'orderId': order_id}
                    )
                    order["amount_original"] = amount_original
                    order["user_amount"] = inner_metadata["user_amount"]
                    order["position_table_id"] = position_table_id
                    success_msg = (
                        f'Order successfully partially closed: ({bound_task}), ({symbol}), ({position_table_id})'
                    )
                    logger.debug(success_msg)
                    return True, order
                except Exception as e:
                    error_msg = f'Failed to partially close: ({bound_task}), ({symbol}), ({position_table_id}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "set_fixed_leverage_for_all_symbols":
                leverage = inner_metadata["leverage"]
                market_id = inner_metadata["market_id"]
                try:
                    await self.exchange.fapiprivate_post_leverage({
                        'symbol': market_id,
                        'leverage': leverage
                    })
                    logger.success(f'Successfully set fixed leverage {leverage} for {market_id}')
                except ccxt.ExchangeError as e:
                    logger.error(f'Failed to set leverage for {market_id}: {e}')
            elif bound_task == "open_multi_orders":
                position_table_id = inner_metadata["position_table_id"]
                symbol = inner_metadata["symbol"]
                side = inner_metadata["side"]
                leverage = inner_metadata["leverage"]
                price = inner_metadata["price"]
                amount = inner_metadata["user_amount"]

                # Used for setting up the leverage
                market = self.exchange.market(symbol)
                market_id = market["id"]

                try:
                    await self.exchange.fapiprivate_post_leverage({
                        'symbol': market_id,
                        'leverage': leverage
                    })
                    logger.success(f'Successfully set leverage {leverage} for {market_id}.')
                except ccxt.ExchangeError as e:
                    error_msg = f'Failed to set leverage for {market_id} while placing a limit order.\n{e}'
                    logger.error(error_msg)
                    return False, error_msg

                try:
                    order = await self.exchange.create_order(symbol, 'limit', side, amount, price, {
                        'type': 'future',
                        'clientOrderId': f'TID_{position_table_id}',
                    })
                    order["position_table_id"] = position_table_id
                    success_msg = f'Order successfully created: ({bound_task}), ({symbol}), ({position_table_id})'
                    logger.debug(success_msg)
                    return True, order
                except ccxt.InsufficientFunds as e:
                    error_msg = f'Failed to create an order - not enough funds\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
                except Exception as e:
                    error_msg = f'Failed to create an order: ({bound_task}), ({symbol}), ({position_table_id}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "calc_balance_availability":

                # because the 'allocation_of_total_balance' is in percetanges inside config.yml
                allocation_of_total_balance = inner_metadata["allocation_of_total_balance"] / 100

                # because the 'allocation_per_single_position' is in percetanges inside config.yml
                allocation_per_single_position = inner_metadata["allocation_per_single_position"] / 100
                try:
                    balance = await self.exchange.fetch_balance()
                    total_balance_in_usdt = balance['total']['USDT']
                    free_balance_in_usdt = balance['free']['USDT']
                except Exception as e:
                    error_msg = f'Failed to fetch balance: ({bound_task}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
                
                balance_to_use_for_trading_in_usdt = total_balance_in_usdt * allocation_of_total_balance
                balance_to_leave_free_in_usdt = total_balance_in_usdt - balance_to_use_for_trading_in_usdt
                usdt_amount_to_use_per_single_position = (
                    balance_to_use_for_trading_in_usdt * allocation_per_single_position
                )
                
                if free_balance_in_usdt > balance_to_leave_free_in_usdt:
                    max_count_of_positions_to_open = int(
                        (free_balance_in_usdt - balance_to_leave_free_in_usdt)
                        // usdt_amount_to_use_per_single_position
                    )
                else:
                    max_count_of_positions_to_open = 0
                
                free_balance_to_use_for_trading_in_usdt = free_balance_in_usdt - balance_to_leave_free_in_usdt
                result = {
                    "balance_to_use_for_trading_in_usdt": balance_to_use_for_trading_in_usdt,
                    "free_balance_to_use_for_trading_in_usdt": free_balance_to_use_for_trading_in_usdt,
                    "usdt_amount_to_use_per_single_position": usdt_amount_to_use_per_single_position,
                    "max_count_of_positions_to_open": max_count_of_positions_to_open
                }
                return True, result
            elif bound_task == "get_last_prices_for_symbols":
                symbol = inner_metadata["market_id"]
                try:
                    ticker = await self.exchange.fetch_ticker(symbol=symbol, params={'type': 'future'})
                    last_price = ticker['last']
                    return True, {"symbol": symbol, "last_price": last_price}
                except Exception as e:
                    error_msg = f'Failed to fetch ticker: ({symbol}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "get_liquidation_prices":
                symbols = inner_metadata["symbols"]
                try:
                    positions = await self.exchange.fetch_positions(symbols=symbols)
                    return True, {"positions": positions}
                except Exception as e:
                    error_msg = f'Failed to fetch positions: ({symbols}).\n{e}'
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "create_sls":
                orig_position_id = inner_metadata["orig_position_id"]
                symbol = inner_metadata["symbol"]
                opossite_side = inner_metadata["opposite_side"]
                sl_price = inner_metadata["sl_price"]
                user_amount = inner_metadata["user_amount"]
                try:
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type="market",
                        side=opossite_side,
                        amount=user_amount,
                        params={"stopPrice": sl_price},
                    )
                    order["orig_position_id"] = orig_position_id
                    order["position_type"] = "sl"
                    order["user_amount"] = inner_metadata["user_amount"]
                    success_msg = (
                        f'Successfully created SL. '
                        f'Symbol: {symbol}, SL price: {sl_price}, Orig. pos. ID: {orig_position_id}.'
                    )
                    logger.debug(success_msg)
                    return True, order
                except Exception as e:
                    error_msg = (
                        f'Failed to create SL. '
                        f'Symbol: {symbol}, SL price: {sl_price}, Orig. pos. ID: {orig_position_id}.\n{e}'
                    )
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "cancel_sls":
                orig_position_id = inner_metadata["orig_position_id"]
                sl_table_id = inner_metadata["sl_table_id"]
                order_id = inner_metadata["position_id"]
                symbol = inner_metadata["symbol"]
                try:
                    order = await self.exchange.cancel_order(
                        id=order_id,
                        symbol=symbol,
                    )
                    order["orig_position_id"] = orig_position_id
                    order["sl_table_id"] = sl_table_id
                    success_msg = (
                        f'Successfully canceled SL. '
                        f'Symbol: {symbol}, Orig. pos. ID: {orig_position_id}, SL table ID: {sl_table_id}'
                    )
                    logger.debug(success_msg)
                    return True, order
                except OrderNotFound as e:
                    order = {
                        "orig_position_id": orig_position_id,
                        "sl_table_id": sl_table_id,
                        "status": "OrderNotFound"
                    }
                    error_msg = (
                        f'Failed to cancel SL (OrderNotFound). '
                        f'Symbol: {symbol}, Orig. pos. ID: {orig_position_id}, SL table ID: {sl_table_id}, '
                        f'SL. pos. ID: {order_id}.\n{e}'
                    )
                    logger.error(error_msg)
                    return True, order
                except Exception as e:
                    error_msg = (
                        f'Failed to cancel SL. '
                        f'Symbol: {symbol}, Orig. pos. ID: {orig_position_id}, SL table ID: {sl_table_id}, '
                        f'SL. pos. ID: {order_id}.\n{e}'
                    )
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "fetch_triggered_sls":
                symbol = inner_metadata["symbol"]
                try:
                    response = await self.exchange.fetch_orders(
                        symbol=symbol,
                        params={'limit': 100}  # You may need to adjust the limit based on your needs
                    )
                except Exception as e:
                    return {"response": e, "symbol": symbol}
                return {"response": response, "symbol": symbol}
            elif bound_task == "fetch_triggered_tps":
                symbol = inner_metadata["symbol"]
                try:
                    response = await self.exchange.fetch_orders(
                        symbol=symbol,
                        params={'limit': 100}  # You may need to adjust the limit based on your needs
                    )
                except Exception as e:
                    return {"response": e, "symbol": symbol}
                return {"response": response, "symbol": symbol}
            elif bound_task == "create_tps":
                orig_position_id = inner_metadata["orig_position_id"]
                symbol = inner_metadata["symbol"]
                opossite_side = inner_metadata["opposite_side"]
                tp_price = inner_metadata["tp_price"]
                user_amount = inner_metadata["user_amount"]
                try:
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type="TAKE_PROFIT_MARKET",
                        side=opossite_side,
                        amount=user_amount,
                        params={"stopPrice": tp_price},
                    )
                    order["orig_position_id"] = orig_position_id
                    order["position_type"] = "tp"
                    order["user_amount"] = inner_metadata["user_amount"]
                    success_msg = (
                        f'Successfully created TP. '
                        f'Symbol: {symbol}, TP price: {tp_price}, Orig. pos. ID: {orig_position_id}.'
                    )
                    logger.debug(success_msg)
                    return True, order
                except Exception as e:
                    error_msg = (
                        f'Failed to create TP. '
                        f'Symbol: {symbol}, TP price: {tp_price}, Orig. pos. ID: {orig_position_id}.\n{e}'
                    )
                    logger.error(error_msg)
                    return False, error_msg
            elif bound_task == "cancel_tps":
                orig_position_id = inner_metadata["orig_position_id"]
                tp_table_id = inner_metadata["tp_table_id"]
                order_id = inner_metadata["position_id"]
                symbol = inner_metadata["symbol"]
                try:
                    order = await self.exchange.cancel_order(
                        id=order_id,
                        symbol=symbol,
                    )
                    order["orig_position_id"] = orig_position_id
                    order["tp_table_id"] = tp_table_id
                    success_msg = (
                        f'Successfully canceled TP. '
                        f'Symbol: {symbol}, Orig. pos. ID: {orig_position_id}, TP table ID: {tp_table_id}'
                    )
                    logger.debug(success_msg)
                    return True, order
                except OrderNotFound as e:
                    order = {
                        "orig_position_id": orig_position_id,
                        "tp_table_id": tp_table_id,
                        "status": "OrderNotFound"
                    }
                    error_msg = (
                        f'Failed to cancel TP (OrderNotFound). '
                        f'Symbol: {symbol}, Orig. pos. ID: {orig_position_id}, TP table ID: {tp_table_id}, '
                        f'TP. pos. ID: {order_id}.\n{e}'
                    )
                    logger.error(error_msg)
                    return True, order
                except Exception as e:
                    error_msg = (
                        f'Failed to cancel TP. '
                        f'Symbol: {symbol}, Orig. pos. ID: {orig_position_id}, TP table ID: {tp_table_id}, '
                        f'TP. pos. ID: {order_id}.\n{e}'
                    )
                    logger.error(error_msg)
                    return False, error_msg

    async def fetch_api_urls(self, bound_task: str, metadata: dict):       
        tasks = []

        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'options': {
                'defaultType': 'future',  # testnet urls work with futures
            }
        })
        # self.exchange.set_sandbox_mode(True)

        if bound_task == "fetch_orders":
            symbols = metadata["symbols"]
            for symbol in symbols:
                inner_metadata = {"symbol": symbol}
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "cancel_multi_orders":
            orders = metadata["orders"]
            for order in orders:
                order_id = order["position_id"]
                symbol = order["symbol"]
                position_table_id = order["id"]
                is_ignored = order["is_ignored"]
                is_ignored_reason = order["is_ignored_reason"]
                trader_id = order["trader_id"]
                roe = order["roe"]
                inner_metadata = {
                    "order_id": order_id,
                    "symbol": symbol,
                    "position_table_id": position_table_id,
                    "is_ignored": is_ignored,
                    "is_ignored_reason": is_ignored_reason,
                    "trader_id": trader_id,
                    "roe": roe
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "close_multi_orders":
            orders = metadata["orders"]
            for order in orders:
                order_id = order["position_id"]
                side = order["side"]
                symbol = order["symbol"]
                user_amount = order["user_amount"]
                position_table_id = order["id"]
                trader_id = order["trader_id"]
                roe = order["roe"]

                inner_metadata = {
                    "order_id": order_id,
                    "side": side,
                    "symbol": symbol,
                    "user_amount": user_amount,
                    "position_table_id": position_table_id,
                    "trader_id": trader_id,
                    "roe": roe
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "partially_close_multi_orders":
            orders = metadata["orders"]
            for order in orders:
                order_id = order["position_id"]
                side = order["side"]
                symbol = order["symbol"]
                amount_original = order["amount"]
                user_amount = order["user_amount"]
                quantity_to_close = order["quantity_to_close"]
                position_table_id = order["id"]

                inner_metadata = {
                    "order_id": order_id,
                    "side": side,
                    "symbol": symbol,
                    "amount_original": amount_original,
                    "user_amount": user_amount,
                    "quantity_to_close": quantity_to_close,
                    "position_table_id": position_table_id
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "set_fixed_leverage_for_all_symbols":
            leverage = metadata["leverage"]
            symbols = await self.exchange.load_markets()
            for symbol in symbols.keys():
                try:
                    market = self.exchange.market(symbol)
                except ccxt.ExchangeError as e:
                    logger.error(e)
                    continue
                inner_metadata = {"leverage": leverage, "market_id": market["id"]}
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "open_multi_orders":
            await self.exchange.load_markets()
            orders = metadata["orders"]
            for order in orders:
                inner_metadata = {
                    "position_table_id": order["id"],
                    "symbol": order["symbol"],
                    "side": order["side"],
                    "leverage": order["leverage"],
                    "price": order["entry_price"],
                    "user_amount": order["user_amount"]
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "calc_balance_availability":
            inner_metadata = {
                "allocation_of_total_balance": metadata["allocation_of_total_balance"],
                "allocation_per_single_position": metadata["allocation_per_single_position"],
            }
            task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
            tasks.append(task)
        elif bound_task == "get_last_prices_for_symbols":
            await self.exchange.load_markets()
            symbols = metadata["symbols"]
            for symbol in symbols:
                try:
                    market = self.exchange.market(symbol)
                except ccxt.ExchangeError as e:
                    logger.error(e)
                    continue
                inner_metadata = {"market_id": market["id"]}
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "get_min_qty_and_step_size_for_symbols":
            markets = await self.exchange.fetch_markets()
            results = {}
            market_ids = metadata["symbols"]  # ["BTCUSDT", "BNBUSDT"]
            for market in markets:
                if market['id'] in market_ids and market['contract'] and not results.get(market['id']):
                    market_filters = market["info"]["filters"]
                    for filter_dict_i in market_filters:
                        filter_type = filter_dict_i["filterType"]
                        if filter_type == "LOT_SIZE":
                            min_qty = filter_dict_i["minQty"]
                            step_size = filter_dict_i["stepSize"]
                            results[market['id']] = {
                                "min_qty": float(min_qty),
                                "step_size": float(step_size)
                            }
                            break
                    # results[market['id']] = market['precision']['amount']
            await self.exchange.close()
            return results
        elif bound_task == "get_liquidation_prices":
            task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=metadata))
            tasks.append(task)
        elif bound_task == "create_sls":
            positions = metadata["positions"]
            sl_ratio = metadata["sl_ratio"]
            for position in positions:
                side = position["side"]
                opposite_side = self.flip_side(side=side)
                if opposite_side == "buy":
                    sl_price = (
                        position["entry_price"]
                        - ((position["entry_price"] - position["liquidation_price"]) * sl_ratio)
                    )
                else:
                    sl_price = (
                        position["entry_price"]
                        + ((position["liquidation_price"] - position["entry_price"]) * sl_ratio)
                    )
                inner_metadata = {
                    "orig_position_id": position["position_id"],
                    "symbol": position["symbol"],
                    "opposite_side": opposite_side,
                    "sl_price": sl_price,
                    "user_amount": position["user_amount"]
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "cancel_sls":
            positions = metadata["positions"]
            for position in positions:
                inner_metadata = {
                    "orig_position_id": position["orig_position_id"],
                    "sl_table_id": position["id"],
                    "position_id": position["position_id"],
                    "symbol": position["symbol"],
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "fetch_triggered_sls":
            symbols = metadata["symbols"]
            for symbol in symbols:
                inner_metadata = {"symbol": symbol}
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "fetch_triggered_tps":
            symbols = metadata["symbols"]
            for symbol in symbols:
                inner_metadata = {"symbol": symbol}
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "create_tps":
            positions = metadata["positions"]
            for position in positions:
                side = position["side"]
                tp_price = position["tp_price"]
                opposite_side = self.flip_side(side=side)
                inner_metadata = {
                    "orig_position_id": position["position_id"],
                    "symbol": position["symbol"],
                    "opposite_side": opposite_side,
                    "tp_price": tp_price,
                    "user_amount": position["user_amount"]
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)
        elif bound_task == "cancel_tps":
            positions = metadata["positions"]
            for position in positions:
                inner_metadata = {
                    "orig_position_id": position["orig_position_id"],
                    "tp_table_id": position["id"],
                    "position_id": position["position_id"],
                    "symbol": position["symbol"],
                }
                task = asyncio.create_task(self.bound_fetch(bound_task=bound_task, inner_metadata=inner_metadata))
                tasks.append(task)

        responses = await asyncio.gather(*tasks)
        await self.exchange.close()
        return responses

    def get_filled_orders_for_multi_symbols(self, metadata: dict):
        bound_task = "fetch_orders"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        filled_orders = []
        for response_i in results:
            if not isinstance(response_i["response"], list) and not isinstance(response_i["response"], dict):
                logger.error(response_i["response"])
                continue
            _symbol = response_i["symbol"]
            inner_responses_list = response_i["response"]
            open_and_partially_filled_orders = [
                order
                for order in inner_responses_list
                if order['info']['status'] in ['FILLED']
            ]
            filled_orders += open_and_partially_filled_orders

        return filled_orders
    
    def get_triggered_sls_for_multi_symbols(self, metadata: dict):
        bound_task = "fetch_triggered_sls"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        look_for_order_ids = metadata["sls_ids"]
        trading_pos_ids = []
        for response_i in results:
            if not isinstance(response_i["response"], list) and not isinstance(response_i["response"], dict):
                logger.error(response_i["response"])
                continue
            inner_responses_list = response_i["response"]
            filled_sls = [
                order["id"]
                for order in inner_responses_list
                if order['info']['status'] in ['FILLED'] and order['id'] in look_for_order_ids
            ]
            trading_pos_ids += filled_sls

        return trading_pos_ids
    
    def get_triggered_tps_for_multi_symbols(self, metadata: dict):
        bound_task = "fetch_triggered_tps"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        look_for_order_ids = metadata["tps_ids"]
        trading_pos_ids = []
        for response_i in results:
            if not isinstance(response_i["response"], list) and not isinstance(response_i["response"], dict):
                logger.error(response_i["response"])
                continue
            inner_responses_list = response_i["response"]
            filled_tps = [
                order["id"]
                for order in inner_responses_list
                if order['info']['status'] in ['FILLED'] and order['id'] in look_for_order_ids
            ]
            trading_pos_ids += filled_tps

        return trading_pos_ids
    
    def open_multi_orders(self, metadata: dict):
        bound_task = "open_multi_orders"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def cancel_multi_orders_v2(self, metadata: dict):
        bound_task = "cancel_multi_orders"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def close_multi_orders_v2(self, metadata: dict):
        bound_task = "close_multi_orders"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def partially_close_multi_orders_v2(self, metadata: dict):
        bound_task = "partially_close_multi_orders"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def change_leverage_for_all_symbols(self, metadata: dict):
        bound_task = "set_fixed_leverage_for_all_symbols"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def calc_balance_availability(self, metadata: dict):
        bound_task = "calc_balance_availability"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results

    def get_last_prices_for_symbols(self, metadata: dict):
        bound_task = "get_last_prices_for_symbols"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        results_fixed = {i[1]["symbol"]: i[1]["last_price"] for i in results if i[0] is True}
        
        return results_fixed
    
    def get_min_qty_and_step_size_for_symbols(self, metadata: dict):
        bound_task = "get_min_qty_and_step_size_for_symbols"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
        
    def flip_side(self, side: str):
        valid_sides = ["buy", "sell"]
        if side not in valid_sides:
            logger.error(f"Invalid side to flip: {side}")
            return False
        
        if side == "buy":
            return "sell"
        else:
            return "buy"
        
    def get_liquidation_prices(self, metadata: dict):
        bound_task = "get_liquidation_prices"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        orig_result = loop.run_until_complete(future)[0]  # [0] because of a single result
        positions = orig_result[1]["positions"] if orig_result[0] else None
        results_fixed = None
        if positions:
            results_fixed = {
                pos["symbol"].replace(":USDT", "").replace("/", ""): pos["liquidationPrice"]
                for pos in positions
            }
        
        if results_fixed:
            return True, results_fixed
        else:
            return orig_result
        
    def create_sls(self, metadata: dict):
        bound_task = "create_sls"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def cancel_sls(self, metadata: dict):
        bound_task = "cancel_sls"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def create_tps(self, metadata: dict):
        bound_task = "create_tps"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
    
    def cancel_tps(self, metadata: dict):
        bound_task = "cancel_tps"

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.fetch_api_urls(bound_task=bound_task, metadata=metadata))
        results = loop.run_until_complete(future)
        
        return results
       

if __name__ == "__main__":
    config = helpers.load_config_from_yaml()
    api_key = config["binance_api_key"]
    api_secret = config["binance_api_secret"]

    trader = TradingAPI(api_key=api_key, api_secret=api_secret)

    orders = [
        {
            "id": 3,
            "symbol": "BTCUSDT",
            "side": "buy",
            "leverage": 2,
            "entry_price": 26096,
            "amount": 0.2
        },
    ]

    # res = asyncio.run(trader.get_filled_orders(symbol="BTCUSDT"))
    # for i in res:
    #     pprint(i, indent=4)

    # symbols = ["BLZUSDT", "BNBUSDT"]
    # results = trader.get_filled_orders_for_multi_symbols(symbols=symbols)
    # print(results)

    # metadata = {"leverage": 4}
    # trader.change_leverage_for_all_symbols(metadata=metadata)

    # metadata = {"orders": orders}
    # trader.open_multi_orders(metadata=metadata)

    # metadata = {
    #     "allocation_of_total_balance": 0.9,
    #     "allocation_per_single_position": 0.05
    #     }
    # res = trader.calc_balance_availability(metadata=metadata)
    # status, result_dict = res[0]
    # print(status, result_dict)

    # metadata = {
    #     "symbols": ["BTCUSDT", "BNBUSDT"]
    # }

    # last_prices = trader.get_last_prices_for_symbols(metadata=metadata)
    # print(last_prices)

    metadata = {"symbols": ["SOLUSDT"]}
    min_qty_and_step_sizes = trader.get_min_qty_and_step_size_for_symbols(metadata=metadata)
    print(min_qty_and_step_sizes)

    # ALLOCATION_OF_TOTAL_BALANCE_RATIO = config["equity_of_total_equity"]
    # ALLOCATION_PER_SINGLE_POSITION_RATIO = config["equity_per_single_pos"]

    # metadata = {
    #     "allocation_of_total_balance": ALLOCATION_OF_TOTAL_BALANCE_RATIO,
    #     "allocation_per_single_position": ALLOCATION_PER_SINGLE_POSITION_RATIO,
    # }

    # res = trader.calc_balance_availability(metadata=metadata)
    # print(res)
