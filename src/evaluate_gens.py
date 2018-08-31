#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 19:40:59 2018
s
@author: Q_X
"""

#'define actual stock price generator'
def actual_generator(actual_file, initial_step):
    with open(actual_file) as actual_hand:
        actual_dict = {}
        act_step_old = initial_step
        act_step_new = initial_step
        while True:
            line = actual_hand.readline()
            if not line:
                act_step_old +=1
                my_dict = actual_dict
                actual_dict = {}
                yield act_step_old-1, my_dict 
            else:
                act_step_old = act_step_new
                act_step_new = int(line.split('|')[0])
                if not act_step_old == act_step_new:
                    my_dict = actual_dict
                    yield act_step_old,my_dict
                stock_name = line.split('|')[1]
                price = float(line.split('|')[2])
                actual_dict[stock_name] = price

#'define predicted stock price generator'
def predict_generator(predict_file, initial_step):
    with open(predict_file) as actual_hand:
        pred_dict = {}
        pred_step_old = initial_step
        pred_step_new = initial_step
        end_of_predict_flag = False
        while True:
            line = actual_hand.readline()
            if not line:
                pred_step_old +=1
                my_dict = pred_dict
                pred_dict = {}
                end_of_predict_flag = True
                yield pred_step_old-1, my_dict, end_of_predict_flag
            else:
                pred_step_old = pred_step_new
                pred_step_new = int(line.split('|')[0])
                if not pred_step_old == pred_step_new:
                    my_dict = pred_dict
                    pred_dict = {}
                    #print(my_dict)
                    yield pred_step_old,my_dict, end_of_predict_flag
                stock_name = line.split('|')[1]
                price = float(line.split('|')[2])
                pred_dict[stock_name] = price

#'function to calculate the list of mean abosolute error'
def calculate_list_of_error(pred_dict, act_dict):
    err_list = []
    for (stock_name, pred_price) in pred_dict.items():
        try:
            act_price = act_dict[stock_name]
        except: 
            act_price = float('nan')
        abs_err = abs(pred_price - act_price)
        err_list.append(abs_err)
    return err_list

#'function to calculate the mean abosolute error and write to file'
def calculate_error_and_write(current_step, err_list):
    step_order = (current_step - pred_initial_step) % window
    window_err_list[step_order] = err_list
    #'start to write AME when steps reach the window'
    if current_step - pred_initial_step + 1 >= window:
        # append all error values into a list
        err_list_all = reduce(lambda x, y: x + y, window_err_list)
        #remove nan values
        err_list_all = [x for x in err_list_all if x==x] 
        #calculate mean error for output
        if len(err_list_all)>0:
            AME = reduce(lambda x,y: x + y, err_list_all) / float(len(err_list_all))
            with open('../output/comparison.txt', 'a') as output_hand:
                output_hand.write('%d|%d|%f\n'%(current_step-window+1,current_step,AME))
        else: 
            AME = 'NA'
            with open('../output/comparison.txt', 'a') as output_hand:
                output_hand.write('%d|%d|%s\n'%(current_step-window+1,current_step,AME))
    return window_err_list

    
#'retrive average error window'
with open('../input/window.txt') as window_hand:
    window = int(window_hand.read())
    
#'retrive the first step in actual stock price'
with open('../input/actual.txt') as actual_hand:
    act_initial_step = int(actual_hand.readline(1).split('|')[0])
    
#'retrive the first step in predicted stock price'
with open('../input/predicted.txt') as predict_hand:
    pred_initial_step = int(predict_hand.readline(1).split('|')[0])
    
#'create output file'
with open('../output/comparison.txt', 'w+') as output_hand:
    pass

err_list = []
window_err_list = [[] for i in range(window)] #list to hold all the abs error
current_step = pred_initial_step #current step to record the correct incremental steps
pred_step_old = pred_initial_step
end_of_predict_flag = False

#'initiate the actual and predicted stock price generator'
act_gen = actual_generator('../input/actual.txt', act_initial_step)
pred_gen = predict_generator('../input/predicted.txt', pred_initial_step)

#'start by loading one batch of actual and predicted stock price
(act_step_old, act_dict) = next(act_gen)
(pred_step_old, pred_dict, end_of_predict_flag) = next(pred_gen)

# main loop #
#'loop through predicted price, and write errors in the comparison file on the go'
while not end_of_predict_flag:
    #if current step equals predicted step, do some comparisons
    if current_step == pred_step_old:
        while act_step_old < current_step:
            (act_step_old, act_dict) = next(act_gen)
        if act_step_old == current_step:
            err_list = calculate_list_of_error(pred_dict, act_dict)
            window_err_list =calculate_error_and_write(current_step, err_list)
            current_step += 1
        elif act_step_old > current_step:
            err_list = []
            window_err_list =calculate_error_and_write(current_step, err_list)
            current_step += 1
            #if current step is smallers, indicate there is data gap in predicted values
    elif current_step < pred_step_old:
        err_list = []
        window_err_list =calculate_error_and_write(current_step, err_list)
        current_step += 1
        #if current step is bigger than pred step, need to load a batch
    elif current_step > pred_step_old:
        (pred_step_old, pred_dict, end_of_predict_flag) = next(pred_gen)
print('finished')