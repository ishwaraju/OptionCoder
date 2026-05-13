"""Decision ladder for trade monitor guidance."""


def monitor_decision(
    signal,
    latest_1m,
    last_two_closes,
    pnl_points,
    pnl_percent,
    stop_loss_pct,
    target_pct,
    partial_trigger_pct,
    runner_trigger_pct,
    dynamic_trail_pct,
    drawdown_from_peak_pct,
    invalidate_underlying_price,
    minutes_active,
    time_stop_warn_minutes,
    time_stop_exit_minutes,
    profit_lock_trigger_pct,
    profit_lock_armed,
    partial_booked,
    momentum_strong,
    momentum_fading,
    option_structure_weak,
    opposite_premium_reexpansion,
    pressure_flip_warning,
    pressure_flip_exit,
    runner_mode,
    momentum_and_pressure_supportive,
    break_even_underlying_exit,
    trail_active_without_partial,
    psar_break,
    psar_style_level,
    slow_positive_theta_risk,
    structure_break,
    vwap_break,
    five_min_break,
    confirmed_flip,
    live_pressure_summary,
    live_pressure_bias,
    entry_pressure_bias,
    no_expansion,
):
    guidance = "HOLD_WITH_TRAIL"
    decision_label = "HOLD_CONTEXT"
    action_text = "Abhi hold karo. Structure abhi toot nahi raha."
    reason = "Normal pullback is okay. Trend is acceptable while the recent 1-minute structure holds."

    if pnl_percent is not None and pnl_percent <= -stop_loss_pct:
        return "EXIT_STOPLOSS", "HARD_STOP_EXIT", "Abhi exit karo. Hard premium stop hit ho gaya.", f"Option premium hit {pnl_percent:.2f}% P&L. Respect the hard {stop_loss_pct:.0f}% loss limit."
    if invalidate_underlying_price is not None and ((signal == "CE" and latest_1m["low"] <= float(invalidate_underlying_price)) or (signal == "PE" and latest_1m["high"] >= float(invalidate_underlying_price))):
        if break_even_underlying_exit and (pnl_percent is not None and pnl_percent > 0):
            return "EXIT_PROFIT_PROTECT", "TRAIL_EXIT", "Profit protect karo aur exit lo. Winner ne follow-through lose kar diya.", f"Trade ne pehle +{profit_lock_trigger_pct:.0f}% zone touch kiya tha, isliye profit lock arm ho gaya. Ab underlying invalidation ke paas close aa gaya hai, to profit protect karke nikalna better hai."
        if confirmed_flip:
            return "EXIT_BIAS", confirmed_flip["label"], confirmed_flip["action_text"], f"Underlying invalidation level {invalidate_underlying_price} got breached. Trade thesis is weakening before premium cap."
        return "EXIT_BIAS", "THESIS_FAILED_EXIT", "Abhi exit karo. Original thesis invalid ho gayi.", f"Underlying invalidation level {invalidate_underlying_price} got breached. Trade thesis is weakening before premium cap."
    if partial_booked and drawdown_from_peak_pct is not None and drawdown_from_peak_pct >= dynamic_trail_pct:
        return "EXIT_TRAIL", "TRAIL_EXIT", "Runner exit karo. Trail hit ho gaya.", f"Runner pulled back {drawdown_from_peak_pct:.2f}% from peak option price. Dynamic trail ({dynamic_trail_pct:.1f}%) hit ho gaya."
    if trail_active_without_partial:
        return "EXIT_TRAIL", "TRAIL_EXIT", "Profit trail hit ho gaya. Winner ko protect karke exit lo.", f"Peak se {drawdown_from_peak_pct:.2f}% pullback aaya after profit-lock activation. Trailing exit ka purpose winner ko hold karna tha, not give it all back."
    if profit_lock_armed and psar_break and (pnl_percent is not None and pnl_percent > 0):
        return "EXIT_PROFIT_PROTECT", "TRAIL_EXIT", "Profit protect exit lo. Ratcheting support toot gaya.", f"PSAR-style trail level {psar_style_level} break ho gaya after profit-lock activation. Trend-following exit ka purpose winner ko hold karna aur reversal par lock-in karna hai."
    if pnl_percent is not None and pnl_percent > 0 and option_structure_weak and opposite_premium_reexpansion and (pressure_flip_warning or pressure_flip_exit or momentum_fading):
        return "THESIS_BROKEN", "THESIS_FAILED_EXIT", "Abhi exit karo. Premium behavior ab trade ke khilaf ja raha hai.", "Option premium structure fail ho raha hai: same-side breadth collapse dikh raha hai, opposite side re-expansion aa rahi hai, aur move ka internal support toot raha hai."
    if partial_booked and pnl_percent is not None and pnl_percent > 0 and option_structure_weak and drawdown_from_peak_pct is not None and drawdown_from_peak_pct >= max(dynamic_trail_pct * 0.7, 6.0):
        return "EXIT_PROTECT", "TRAIL_EXIT", "Bacha hua size bhi protect karo aur exit lo.", f"Peak se {drawdown_from_peak_pct:.2f}% give-back aa gaya hai aur option structure weak ho gaya. Ab remainder ko protect karna better hai."
    if runner_mode and pnl_percent is not None and pnl_percent >= runner_trigger_pct and momentum_and_pressure_supportive:
        return "HOLD_STRONG", "LET_WINNER_RUN", "Yeh runner lag raha hai. Trail ke saath hold karo, jaldi partial mat karo.", f"Trade ne +{runner_trigger_pct:.0f}% runner zone touch kiya hai aur premium expansion supportive hai. Is type ke breakout winner ko trail ke saath chalne dena better hai."
    if not partial_booked and pnl_percent is not None and pnl_percent >= max(partial_trigger_pct * 0.6, 8.0) and option_structure_weak and not runner_mode:
        return "TRIM", "BOOK_PARTIAL_NOW", "Kuch size trim karo. Move mila hai, par premium behavior weak pad raha hai.", "Option ne profit diya hai, lekin 3-bar premium momentum, spread ya breadth support ab utna clean nahi hai. Is zone me partial trim karna option buyer ke liye disciplined hai."
    if not partial_booked and pnl_percent is not None and pnl_percent >= target_pct and momentum_and_pressure_supportive:
        return "HOLD_STRONG", "HOLD_STRONG", "Profit ko chalne do. Trail active hai, jaldi exit mat karo.", f"Target zone (+{target_pct:.0f}%) hit ho chuka hai aur momentum abhi supportive hai. Trailing-stop logic ke hisaab se winner ko run karne dena better hai."
    if not partial_booked and pnl_percent is not None and pnl_percent >= partial_trigger_pct and not runner_mode:
        return "BOOK_PARTIAL", "BOOK_PARTIAL_NOW", "Abhi partial book karo. Runner ko trail par chhodo.", f"Option premium reached {pnl_percent:.2f}% P&L. Partial booking near +{partial_trigger_pct:.0f}% objective option buyer ke liye safer hai."
    if slow_positive_theta_risk:
        return "EXIT_PROFIT_PROTECT", "TRAIL_EXIT", "Small profit ko protect karo. Slow option move me theta risk badh raha hai.", f"Trade profit me hai but {time_stop_warn_minutes} min ke andar move decisive nahi bana. Option buyer ke liye aise slow winners jaldi fade ho sakte hain, isliye protect karna better hai."
    if minutes_active >= time_stop_exit_minutes and no_expansion and (pnl_percent is None or pnl_percent <= 0):
        return "EXIT_TIMESTOP", "TIME_STOP_EXIT", "Abhi exit karo. Move expand nahi hua aur time nikal gaya.", f"{time_stop_exit_minutes} min ke baad bhi move expand nahi hua. Time stop exit better hai than waiting for theta bleed."
    if minutes_active >= time_stop_warn_minutes and no_expansion:
        return "TIME_DECAY_RISK", "HIGH_NOISE_SKIP", "Fresh add mat karo. Setup abhi noisy hai aur premium expand nahi kar raha.", f"{time_stop_warn_minutes} min ke andar meaningful expansion nahi aayi. Aise slow option trades theta bleed me badal sakte hain."
    if pressure_flip_exit:
        pressure_text = (live_pressure_summary or {}).get("summary") or live_pressure_bias
        if confirmed_flip:
            return "EXIT_BIAS", confirmed_flip["label"], confirmed_flip["action_text"], f"Entry ke baad pressure bias {entry_pressure_bias or 'UNKNOWN'} se flip hokar {live_pressure_bias or 'OPPOSITE'} ho gaya. {pressure_text}. Jab price structure aur options participation saath na dein, thesis fail maana better hai."
        return "EXIT_BIAS", "THESIS_FAILED_EXIT", "Abhi exit karo. Live option pressure trade ke opposite flip ho gaya.", f"Entry ke baad pressure bias {entry_pressure_bias or 'UNKNOWN'} se flip hokar {live_pressure_bias or 'OPPOSITE'} ho gaya. {pressure_text}. Jab price structure aur options participation saath na dein, thesis fail maana better hai."
    if structure_break and vwap_break:
        if confirmed_flip:
            return "EXIT_BIAS", confirmed_flip["label"], confirmed_flip["action_text"], "Recent 1-minute structure broke with VWAP loss. This is more than a normal pullback."
        return "EXIT_BIAS", "THESIS_FAILED_EXIT", "Abhi exit karo. Structure aur VWAP dono break ho gaye.", "Recent 1-minute structure broke with VWAP loss. This is more than a normal pullback."
    if five_min_break and minutes_active >= 3:
        return "EXIT_BIAS", "THESIS_FAILED_EXIT", "Abhi cautious exit lo. 5m structure against ja raha hai.", "Previous 5-minute structure broke against your trade. Momentum may be losing control."
    if momentum_fading and (pnl_points is not None and pnl_points > 0):
        if profit_lock_armed:
            return "HOLD_WITH_TRAIL", "HOLD_CONTEXT", "Profit me ho. Trail active rakho, random profit booking mat karo.", "Momentum thoda slow hua hai, lekin profit-lock arm ho chuka hai. Winner ko trail ke saath hold karna jaldi exit se better hai."
        return "BOOK_PARTIAL", "BOOK_PARTIAL_NOW", "Profit me ho. Partial book karna safer hai.", "Profit is available and momentum is slowing. Partial booking can reduce pressure."
    if momentum_strong and (pnl_points is not None and pnl_points > 0):
        return "HOLD_STRONG", "HOLD_STRONG", "Abhi hold karo. Momentum strong hai.", "Fresh momentum is expanding. Do not react to a normal 1-minute pullback."
    if momentum_fading:
        return "MOMENTUM_PAUSE", "WAIT_CONFIRMATION", "Abhi wait karo. Momentum pause hai, flip abhi confirm nahi hai.", "Momentum paused, but this still looks like a normal pullback unless structure breaks."
    if pressure_flip_warning:
        pressure_text = (live_pressure_summary or {}).get("summary") or live_pressure_bias
        return "THESIS_WEAKENING", "WAIT_CONFIRMATION", "Trade weak ho raha hai. Exit ke liye prepared raho agar next candle support na kare.", f"Live pressure ab trade ke opposite side ja raha hai: {pressure_text}. Abhi hard exit nahi, lekin thesis weaken ho rahi hai."
    if profit_lock_armed and psar_break:
        return "THESIS_WEAKENING", "WAIT_CONFIRMATION", "Profit trail ke paas ho. Agar next candle recover na kare to exit lo.", f"PSAR-style trail level {psar_style_level} ke paas price aa gaya hai. Winner ko hold karo, but give-back ko ignore mat karo."
    if len(last_two_closes) == 2 and ((signal == "CE" and last_two_closes[-1] <= last_two_closes[-2]) or (signal == "PE" and last_two_closes[-1] >= last_two_closes[-2])):
        return "NORMAL_PULLBACK", "HOLD_CONTEXT", "Small pullback normal hai. Abhi panic exit mat karo.", "A small opposite candle is normal. No real structure damage yet."
    return guidance, decision_label, action_text, reason
