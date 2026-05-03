# Option Buyer Exit Rules

Ye note simple Hinglish me explain karta hai ki signal aane ke baad, agar hum maan lein ki trade le liya gaya hai, to bot CE/PE buyer trade ko kaise manage karega.

## Core Soch

Option buyer ko 3 cheezein chahiye:

- sahi direction
- sahi timing
- theta decay se bachav

Isliye bot ek random exit use nahi karta. Ab layered exit stack use hota hai.

## Exit Stack

### 1. Thesis Failed Exit

Trade tab galat maana jayega jab original underlying thesis toot jaye.

`CE` trade ke liye:

- underlying invalidation level break ho jaye
- 1m structure aur VWAP dono toot jayein
- 5m structure trade ke against chala jaye
- live option pressure strong bearish flip ho jaye

`PE` trade ke liye yehi logic ulta hoga.

Ye usually message me aise dikhega:

- `EXIT NOW`
- `THESIS FAILED EXIT`

### 2. Hard Premium Stop

Agar option premium khud predefined loss cap tak gir jaye, bot exit dega.

Typical behavior:

- breakout/retest me stop tighter hota hai
- reversal me thoda wider hota hai
- expiry mode me aur fast/tight behavior aata hai

Ye message me aise dikhega:

- `EXIT NOW`
- `HARD STOP EXIT`

### 3. Time Stop

Option buyer hamesha wait nahi kar sakta.

Agar move expected time window ke andar expand nahi hota, trade dangerous ho jata hai kyunki theta hurt karna start kar deta hai.

Ye message me aise dikhega:

- `WATCH CLOSELY`
- `EXIT NOW`
- `TIME DECAY RISK`
- `TIME STOP EXIT`

### 4. Profit Lock

Agar trade meaningful profit zone me chala jata hai, bot `profit-lock` arm kar deta hai.

Iska matlab:

- trade ab fresh entry jaisa manage nahi hoga
- system ab winner ko protect karega

Profit-lock trigger setup-aware aur expiry-aware hota hai.

Example:

- breakout me lock jaldi arm ho sakta hai
- expiry mode me aur jaldi ho sakta hai

### 5. Let Winner Run

Agar:

- profit target zone touch ho gaya
- momentum abhi bhi strong hai
- pressure supportive hai
- trail break nahi hua

to bot jaldi profit booking force nahi karega.

Ye message me aise dikhega:

- `LET WINNER RUN`
- `HOLD STRONG`

Matlab:

- sirf green dekh ke exit mat karo
- jab tak trail aur thesis safe hain, winner ko chalne do

### 6. Trail Active

Jab profit-lock arm ho jata hai, trade ko normal pauses me bhi hold kiya ja sakta hai.

Agar momentum thoda slow ho jaye lekin trade structurally theek ho, to bot trail protection ke saath hold karne bolega.

Ye message me aise dikhega:

- `TRAIL ACTIVE`
- `HOLD WITH TRAIL`

Matlab:

- panic booking nahi
- random exit nahi
- winner ko chalne do, but give-back control me rakho

### 7. Profit Protect Exit

Agar profitable trade profit-lock ke baad quality lose karna start kare, bot gains protect karne ke liye exit dega.

Ye in cases me ho sakta hai:

- underlying invalidation ke paas wapas aa jaye
- dynamic trail hit ho jaye
- PSAR-style ratcheting level toot jaye
- slow winner high theta-risk zone me chala jaye

Ye message me aise dikhega:

- `PROFIT LOCK EXIT`
- `EXIT PROFIT PROTECT`

Matlab:

- trade accha tha
- ab best phase shayad nikal gaya
- ab paise bachake nikalna better hai

## Option Buyer Specific Bias

Bot ko jaan-bujhkar option buyer ke liye stricter rakha gaya hai.

Kyun:

- long options time decay se hurt hote hain
- slow moves dangerous hote hain
- late-day aur expiry moves jaldi fade ho sakte hain

Isliye buyer mode me:

- late-day behavior tighter hota hai
- expiry behavior faster hota hai
- theta-risk context me profit-lock jaldi arm hota hai
- slow positive trades ko bhi protect kiya jata hai

## Message Ka Matlab

### `LET WINNER RUN`

Move achha chal raha hai. Sirf profit dikh raha hai isliye jaldi exit mat karo.

### `TRAIL ACTIVE`

Trade profit me hai aur protection on hai. Jab tak trail ya thesis break na ho, hold theek hai.

### `WATCH CLOSELY`

Trade weaken ho raha hai ya proper expand nahi kar raha. Next candle dhyan se dekho.

### `PROFIT LOCK EXIT`

Winner ko ab protect karna chahiye. Exit lo aur gains save karo.

### `EXIT NOW`

Setup fail ho gaya, stop hit ho gaya, ya trade ab aur time deserve nahi karta.

## Practical Summary

Bot ka buyer-first behavior ab ye hai:

- losers ko jaldi cut karo
- slow dead trades ko reject karo
- fast winners ko run karne do
- profitable trades ko trail se protect karo
- winner structure ya timing lose kare to nikal jao

Ye specifically us common option-buyer problem ko fix karne ke liye hai:

- loss me zyada wait karna
- profit me bahut jaldi exit kar dena
