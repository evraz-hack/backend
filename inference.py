import glob
import catboost as cb
import numpy as np
import pickle

with open('models/lim_mapping.pkl', 'rb') as f:
    lim = pickle.load(f)

models = glob.glob('models/*.cb')

def run(df):
    for model_path in models:
        model = cb.CatBoostRegressor()
        model.load_model(model_path)

        col_name = model_path.split('/')[-1].split('.')[0]

        preds = df[col_name].values[-100:].tolist()
        for i in range(300):
            pred = model.predict(preds[-100:])
            preds.append(pred)

        preds = np.array(preds[100:])
        alarm_max, alarm_min, warning_max, warning_min = lim[col_name].values()
        is_crashed = -1
        is_warning = -1
        for i in range(len(preds)):
            if preds[i] > alarm_max or preds[i] < alarm_min:
                is_crashed = i
                break
            elif preds[i] > warning_max or preds[i] < warning_min:
                is_warning = i

        if is_crashed != -1:
            print('alarm',  (is_crashed * 5) // 60 + 1)
        elif is_warning != -1:
            print('warning', (is_crashed * 5) // 60 + 1)
        else:
            print('empty', -1)
