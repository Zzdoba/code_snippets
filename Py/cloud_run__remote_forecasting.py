import os
from flask import Flask, request, jsonify
import pandas as pd
from prophet import Prophet
import datetime

app = Flask(__name__)


def process_calls(calls: list):

    # getting data from request
    ds, y = [], []
    for call in calls:
        ds.append(datetime.datetime.strptime(call[0], '%Y-%m-%d'))
        y.append(call[1])
        interval_width = call[2]

    # making dataframe
    df = pd.DataFrame({'ds': ds, 'y': y})
    data = df.sort_values('ds')
    data_train = data[data['ds'] < data['ds'].max()]

    # making prediction
    m = Prophet(interval_width=interval_width)
    m.fit(data_train)
    future = m.make_future_dataframe(periods=1)
    forecast = m.predict(future)
    df_predict = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    df_full = df_predict.merge(data, how='left', on='ds')

    # making & returning result
    result = []
    for i in range(len(df_full)):
        result.append({
            'ds': df_full['ds'][i].strftime('%Y-%m-%d'),
            'y': float(df_full['y'][i]),
            'yhat': float(df_full['yhat'][i]),
            'yhat_lower': float(df_full['yhat_lower'][i]),
            'yhat_upper': float(df_full['yhat_upper'][i])
        })
    return result


@app.route("/", methods=['POST'])
def batch_add():
    try:
        request_json = request.get_json()
        calls = request_json['calls']
        return jsonify({"replies": process_calls(calls)})
    except Exception:
        return jsonify({"errorMessage": 'something unexpected in input'}), 400


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
