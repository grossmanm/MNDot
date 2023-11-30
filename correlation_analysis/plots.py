import matplotlib.pyplot as plt
import numpy as np

def plot_result(method, trues, preds=[]):
    x = np.array([i for i in range(len(trues))])

    if len(preds) == 0:
        plt.plot(x, trues)
    else:
        plt.plot(x, preds, '+', x, trues, '-')

    plt.title(method, fontsize='x-large')
    plt.legend(['pred', 'true'], shadow=True, loc='best')
    plt.show()

def plot_result_with_name(method, trues, name):
    x = np.array([i for i in range(len(trues))])


    plt.plot(x, trues)

    plt.title(method, fontsize='x-large')
    plt.legend([name], shadow=True, loc='best')
    plt.show()

def plot_result1(method, trues, preds=[]):
    x = np.array([i for i in range(len(trues))])

    if len(preds) == 0:
        plt.plot(x, trues)
    else:
        plt.plot(x, preds, '-', x, trues, '-')

    plt.title(method, fontsize='x-large')
    plt.legend(['pred', 'true'], shadow=True, loc='best')
    plt.show()

def plot_result_multi_color(method, cut_points, trues, preds=[]):


    if len(preds) == 0:
        x = np.array([i for i in range(len(trues))])
        plt.plot(x, trues)
    else:
        if len(cut_points) > 1:
            x1 = np.array([i for i in range(cut_points[0])])
            x2 = np.array([i for i in range(cut_points[0]-1, cut_points[1])])
            x3 = np.array([i for i in range(cut_points[1]-1, len(trues))])

            plt.plot(x1, preds[:cut_points[0]], 'b+', x1, trues[:cut_points[0]], 'r-')
            plt.plot(x2, preds[cut_points[0]-1:cut_points[1]], 'b+',
                     x2, trues[cut_points[0]-1:cut_points[1]], 'y-')
            plt.plot(x3, preds[cut_points[1]-1:len(trues)], 'b+',
                     x3, trues[cut_points[1]-1:len(trues)], 'k-')
        else:
            x1 = np.array([i for i in range(cut_points[0])])
            x2 = np.array([i for i in range(cut_points[0]-1, len(trues))])

            plt.plot(x1, preds[:cut_points[0]], 'b+', x1, trues[:cut_points[0]], 'r-')
            plt.plot(x2, preds[cut_points[0]-1:], 'b+', x2, trues[cut_points[0]-1:], 'k-')


    plt.title(method, fontsize='x-large')
    # plt.legend(['pred', 'true'], shadow=True, loc='best')
    plt.show()

def plot_loc_corr(s1, name1, s2, name2, score0):
    fig, axes = plt.subplots(2, 1)
    fig.subplots_adjust(hspace=0.5)
    axes[0].plot(range(len(s1)), s1, range(len(s2)), s2)
    axes[0].legend([name1, name2], shadow=True, loc='best')
    axes[1].plot(score0, lw=0.6)
    axes[1].set_title(name1 + ' vs ' + name2)
    # plt.show()
    plt.savefig('./plot/' + name2+'_local_correlation_Dec_Jan_2022_694_eriver_nramp.png')

if __name__ == '__main__':
    x = range(10)
    y = [0,1,2,3,4,5,6,7,8,9]
    z = [0,-1,-2,-3,-4,-5,-6,-7,-8,-9]
    # for x1,  y1 in zip(x, y):
    #     if x1 < 5:
    #         plt.plot(x1, y1, 'r')
        # elif y1 < y2:
        #     plt.plot([x1, x2], [y1, y2], 'g')
        # else:
        #     plt.plot(x1, y1, 'b')
    plt.plot(x[:5], y[:5], 'r')
    plt.plot(x[4:], y[4:], 'g')
    plt.plot(x, z, 'b')
    plt.show()

def list_move_left(A, a):
    inserted_val = max(A)
    for i in range(a):
        A.insert(len(A), inserted_val)
        A.remove(A[0])
    return A

