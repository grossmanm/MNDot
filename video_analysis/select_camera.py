def camera_to_use(camera_name):
    # some locations do not have the maximum of 4 camera views and only use 2 or 3.
    # Others are fisheye lenses and cannot be split into quadrants like the others
    split = True
    use_tr = True
    use_tl = True
    use_br = True
    use_bl = True
    if camera_name=='65_81st_Vision_Stream1':
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    elif camera_name=='694_eriver_nramp_vision':
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    elif camera_name=='36_whitebear_nramp_iteris':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name=='36_whitebear_sramp_iteris':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name=='65_Blake_Vision_Stream1':
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    elif camera_name=='35e_cliff_eramp_iteris':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name=='35e_cliff_wramp_iteris':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name=='s_cr144_rogershighscool_vision':
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    elif camera_name == '62_france_sramp_vision':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name == '62_france_nramp_vision':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name == '77_cliff_wramp_vision':
        split = True
        use_tr = True
        use_tl = True
        use_br = False
        use_bl = True
    elif camera_name == 'CR81_deere_visions_stream':
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    elif camera_name == 'CR81_industrial_visions_stream':
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    elif 'gridsmart' in camera_name:
        split = False
        use_tr = False
        use_tl = False
        use_br = False
        use_bl= False
    else:
        split = True
        use_tr = True
        use_tl = True
        use_br = True
        use_bl = True
    return split, use_tr, use_tl, use_br, use_bl