import numpy as np
import matplotlib.pyplot as plt

# Параметры схемы
R = 1000       # Сопротивление резистора R, Ом
U0 = 5.0       # Напряжение пробоя сборки, В
r_dyn = 500    # Динамическое сопротивление стабилитронов, Ом
U_max = 15     # Максимальное входное напряжение для графика

u_in = np.linspace(-U_max, U_max, 1000)

# 1. Линия чистого резистора (для ориентира, как на Рис. 5б)
i_res = u_in / R

# 2. В) Идеальная ВАХ схемы
i_ideal = np.zeros_like(u_in)
mask_p = u_in > U0
mask_n = u_in < -U0
i_ideal[mask_p] = (u_in[mask_p] - U0) / R
i_ideal[mask_n] = (u_in[mask_n] + U0) / R

# 3. Б) Кусочно-линеаризованная ВАХ схемы (R + r_dyn)
i_linear = np.zeros_like(u_in)
i_linear[mask_p] = (u_in[mask_p] - U0) / (R + r_dyn)
i_linear[mask_n] = (u_in[mask_n] + U0) / (R + r_dyn)

# 4. А) Реальная ВАХ схемы (с плавным переходом)
def real_circuit_vax(u, u0, r_total, vt=0.4):
    # Используем softplus для плавности
    return np.sign(u) * np.log1p(np.exp((np.abs(u) - u0) / vt)) * vt / r_total

i_real = real_circuit_vax(u_in, U0, R + r_dyn)

# Отрисовка
plt.figure(figsize=(10, 7))

# Вспомогательная линия резистора (серая пунктирная)
plt.plot(u_in, i_res * 1000, color='gray', linestyle='--', alpha=0.5, label='Линия резистора $1/R$')

# Основные графики
plt.plot(u_in, i_ideal * 1000, 'b-', linewidth=2, label='в) Идеальная (наклон $1/R$)')
plt.plot(u_in, i_linear * 1000, 'g--', linewidth=2, label='б) Линеаризованная (наклон $1/(R+r_{обр})$)')
plt.plot(u_in, i_real * 1000, 'r-', linewidth=2, label='а) Реальная (плавное "колено")')

# Оформление
plt.axhline(0, color='black', lw=1)
plt.axvline(0, color='black', lw=1)
plt.title('ВАХ всей схемы ограничителя ($i$ от $u_{вх}$) в единых осях', fontsize=12)
plt.xlabel('Входное напряжение $u_{вх}$, В')
plt.ylabel('Ток в цепи $i$, мА')
plt.legend()
plt.grid(True, linestyle=':', alpha=0.6)

# Отметки U0
plt.xticks([-U0, 0, U0], ['$-U_0$', '0', '$U_0$'])
plt.xlim(-U_max, U_max)
plt.ylim(min(i_ideal*1000)-1, max(i_ideal*1000)+1)

plt.show()