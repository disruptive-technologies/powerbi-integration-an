# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset = pandas.DataFrame(temperature)
# dataset = dataset.drop_duplicates()

# packages
import numpy             as np
import pandas            as pd
import matplotlib.pyplot as plt

# model coefficients map
# relate the last few digits in a sensor id with a coefficient
coeffs = {
    'default':  0.01,
    'hh0':      0.05,
    'uc0':      0.01,
    'jo0':      0.05,
}

# Newton's Law of Cooling
def nlc_model(x, k):
    # initialise output
    m = np.zeros(len(x))

    # iterate temperature values
    for i in range(len(x)):
        if i == 0:
            # zero index case
            m[i] = x[i]
        else:
            # update model
            dt   = -k*(m[i-1] - x[i])
            m[i] = m[i-1] + dt
    
    return m

# set timestamp as index column and convert to datetime object
dataset['timestamp'] = pd.to_datetime(dataset['timestamp'])
# dataset = dataset.set_index('timestamp', drop=True)

# find unique identifiers in dataset
unique_ids = np.unique(dataset['device_id'])

# iterate ids
for uid in unique_ids:
    # isolate data for id
    timestamps  = dataset['timestamp'].iloc[np.where(dataset['device_id']==uid)[0]].values
    temperature = dataset['temperature'].iloc[np.where(dataset['device_id']==uid)[0]].values

    # select coefficient based on mapping
    k = coeffs['default']
    for key in coeffs.keys():
        if uid.endswith(key):
            k = coeffs[key]

    # model the temperature with Newton's Law of Cooling
    model = nlc_model(temperature, k=k)

    plt.plot(timestamps, temperature, label=uid[-4:] + ', k={}'.format(k))
    plt.plot(timestamps, model, '--k', label='model')

# get rid of the frame
for spine in plt.gca().spines.values():
    spine.set_visible(False)

plt.legend()
plt.grid(axis='y', linestyle=':')
plt.xlabel('timestamp')
plt.ylabel('temperature')
plt.title('Temperature Inertia Model')
plt.tight_layout()
plt.show()
