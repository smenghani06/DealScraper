(function($){
    //jQuery time
    var current_fs, next_fs, previous_fs; //fieldsets
    var left, opacity, scale; //fieldset properties which we will animate
    var animating; //flag to prevent quick multi-click glitches
    var has_response = false;
    var preview_clicked = false;

    jQuery(document).ready(function($) {
        $('form.wpcf7-form').each(function(index, el) {
            var totalFieldset = 0;
            var this_form = $(el);
            var findFieldset = $(el).find('fieldset.fieldset-cf7mls');
            if (findFieldset.length > 0) {
                $.each(findFieldset, function(i2, el2) {
                    if (i2 == 0) {
                        $(el2).addClass('cf7mls_current_fs');
                    }

                    jQuery(el2).attr('data-cf7mls-order', i2);
                    totalFieldset = totalFieldset + 1;
                    //disable next button if the fieldset has  wpcf7-acceptance
                    var acceptances = jQuery(el2).find('input:checkbox.wpcf7-acceptance');
                    if (acceptances.length) {
                        cf7mls_toggle_next_btn(acceptances, el2);
                    }
                });
                $.each(findFieldset, function(i2, el2) {
                    if (i2 == (totalFieldset - 1)) {
                        $(el2).find('.cf7mls_next').remove();
                    }
                });
                $(el).attr('data-count-fieldset', totalFieldset);
                //on form submit
                $(el).submit(function(event) {
                    var findFieldset = $(el).find('fieldset.fieldset-cf7mls.cf7mls_current_fs');
                    if (findFieldset.data('cf7mls-order') != (totalFieldset - 1)) {
                        findFieldset.find('.cf7mls_next').click();
                        return false;
                    }                
                });
            }
            //answer fields
            $(el).find('input.wpcf7-cf7_answer').each(function(njt_a_i, njt_a_e) {
                var $this = $(njt_a_e);
                var qf = this_form.find('[name="'+$this.data('qf')+'"]');
                var sp = $('span.wpcf7-cf7_answer_' + $this.data('qf'));
                qf.on('change', function(event) {
                    event.preventDefault();
                    var _val = $(this).val();
                    sp.text(_val);
                    $this.val(_val);
                });
            });
        });

    });
    jQuery(document).on('click', 'form.wpcf7-form input:checkbox.wpcf7-acceptance', function(event) {
        //event.preventDefault();
        var $this = jQuery(this);
        var parent_fieldset = $this.closest('fieldset.fieldset-cf7mls');
        if (parent_fieldset.length) {
            var acceptances = jQuery(parent_fieldset).find('input:checkbox.wpcf7-acceptance');
            if (acceptances.length) {
                cf7mls_toggle_next_btn(acceptances, parent_fieldset);
            }            
        }
    });

    $(document).on('click', '.cf7mls_next', function(event) {
        event.preventDefault();
        var $this = $(this);

        $this.addClass('sending');
        current_fs = $this.closest('.fieldset-cf7mls');
        next_fs = current_fs.next();

        //validation
        var form = $this.parent().closest('form.wpcf7-form');

        var fd = new FormData();
        $.each(form.find('input[type="file"]'), function(index, el) {

            fd.append($(el).attr('name'), $(el)[0].files[0]);
        });
        
        var other_data = form.serializeArray();
        $.each(other_data,function(key, input){
            fd.append(input.name, input.value);
        });


        $.ajax({
            url: cf7mls_object.ajax_url + '?action=cf7mls_validation',
            type: 'POST',
            data: fd,
            processData: false,
            contentType: false,
        })
        .done(function(msg) {
            $this.removeClass('sending');
            var json = $.parseJSON(msg);

            /*
             * Insert _form_data_id if 'json variable' has
             */
            if (typeof json._cf7mls_db_form_data_id != 'undefined') {
                if (!form.find('input[name="_cf7mls_db_form_data_id"]').length) {
                    form.append('<input type="hidden" name="_cf7mls_db_form_data_id" value="'+json._cf7mls_db_form_data_id+'" />');
                }
            }            
            
            if (!json.success) {
                var checkError = 0;
                //reset error messages
                current_fs.find('.wpcf7-form-control-wrap').removeClass('cf7mls-invalid');
                current_fs.find('.wpcf7-form-control-wrap .wpcf7-not-valid-tip').remove();
                if (has_response) {                    
                    current_fs.find('.wpcf7-response-output.wpcf7-validation-errors').removeClass('wpcf7-validation-errors');
                } else {
                    current_fs.find('.wpcf7-response-output.wpcf7-validation-errors').remove();
                }
                
                $.each(json.invalid_fields, function(index, el) {
                    if (current_fs.find('input[name="'+index+'"]').length || 
                        current_fs.find('input[name="'+index+'[]"]').length ||
                        current_fs.find('select[name="'+index+'"]').length || 
                        current_fs.find('select[name="'+index+'[]"]').length ||
                        current_fs.find('textarea[name="'+index+'"]').length || 
                        current_fs.find('textarea[name="'+index+'[]"]').length
                    ) {
                        checkError = checkError + 1;

                        var controlWrap = $('.wpcf7-form-control-wrap.' + index, form);
                        controlWrap.addClass('cf7mls-invalid');
                        controlWrap.find('span.wpcf7-not-valid-tip').remove();
                        controlWrap.append('<span role="alert" class="wpcf7-not-valid-tip">' + el.reason + '</span>');

                        //return false;
                    }
                });

                if (checkError == 0) {
                    json.success = true;
                    has_response = false;
                } else {
                    if (current_fs.find('.wpcf7-response-output').length) {
                        has_response = true;
                        current_fs.find('.wpcf7-response-output').addClass('wpcf7-validation-errors').show().text(json.message);
                    } else {
                        has_response = false;
                        current_fs.append('<div class="wpcf7-response-output wpcf7-display-none wpcf7-validation-errors" style="display: block;" role="alert">' + json.message + '</div>');
                    }
                    
                }
            }

            if (json.success) {
                
                /*
                current_fs.fadeOut('400', function() {
                    next_fs.fadeIn('400');
                });
                
                });
                */
               
                //reset error messages
                current_fs.find('.wpcf7-form-control-wrap').removeClass('cf7mls-invalid');
                current_fs.find('.wpcf7-form-control-wrap .wpcf7-not-valid-tip').remove();
               
               /*
                * Instead of hiding current fs, we hide all
                */
               /*
                current_fs.css({
                    height: '0px',
                    overflow: 'hidden',
                    opacity: '0',
                    'visibility': 'hidden'
                }).removeClass('cf7mls_current_fs');
                */
                form.find('.fieldset-cf7mls').css({
                    height: '0px',
                    overflow: 'hidden',
                    opacity: '0',
                    'visibility': 'hidden'
                }).removeClass('cf7mls_current_fs');

                next_fs.css({
                    height: 'auto',
                    overflow: 'visible',
                    opacity: '1',
                    'visibility': 'visible'
                }).addClass('cf7mls_current_fs');

                dhScrollTo(form);

                return false;
                
            } else {

            }
        })
        .fail(function() {
            $this.removeClass('sending');
            console.log("Validation error");
        })
        .always(function() {
            $this.removeClass('sending');
            console.log("Validation complete");
        });
        return false;
    });

    $(".cf7mls_back").click(function(){
        var $this = $(this);
        var form = $this.parent().closest('form.wpcf7-form');

        $('.wpcf7-response-output.wpcf7-display-none').removeClass('wpcf7-validation-errors').html('');
        $('.wpcf7-response-output.wpcf7-display-none.wpcf7-mail-sent-ok').hide();
        
        current_fs = $this.closest('.fieldset-cf7mls');
        previous_fs = current_fs.prev();
        
        //reset error messages
        current_fs.find('.wpcf7-form-control-wrap').removeClass('cf7mls-invalid');
        current_fs.find('.wpcf7-form-control-wrap .wpcf7-not-valid-tip').remove();
        
        /*
         * Instead of hide current fs, we hide all
         */
        /*current_fs.css({
            height: '0px',
            overflow: 'hidden',
            opacity: '0',
            'visibility': 'hidden'
        }).removeClass('cf7mls_current_fs');*/
        form.find('.fieldset-cf7mls').css({
            height: '0px',
            overflow: 'hidden',
            opacity: '0',
            'visibility': 'hidden'
        }).removeClass('cf7mls_current_fs');

        previous_fs.css({
            height: 'auto',
            overflow: 'visible',
            opacity: '1',
            'visibility': 'visible'
        }).addClass('cf7mls_current_fs');

        dhScrollTo(form);

        return false;
    });

    /*
     * Preview Form Clicked
     */
    $(document).on('click', '.wpcf7-cf7mls_preview_step', function(event) {
        event.preventDefault();
        var $this = $(this);
        var form = $this.closest('form.wpcf7-form');        

        if (preview_clicked === false) {
            form.find('.cf7mls_back').hide();
            form.find('.cf7mls_next').hide();

            form.find('.fieldset-cf7mls').css({
                height: 'auto',
                overflow: 'visible',
                opacity: '1',
                'visibility': 'visible'
            });
            form.find('input').attr('disabled', 'disabled');
            form.find('button').attr('disabled', 'disabled');
            form.find('select').attr('disabled', 'disabled');
            form.find('textarea').attr('disabled', 'disabled');
            form.find('.wpcf7-cf7mls_preview_step').removeAttr('disabled');

            form.addClass('cf7mls_reviewing');
            $('body').append('<div class="cf7mls_reviewing_overlay"></div>');
        } else {
            form.find('.fieldset-cf7mls').css({
                height: '0px',
                overflow: 'hidden',
                opacity: '0',
                'visibility': 'hidden'
            });

            form.find('.cf7mls_back').show();
            form.find('.cf7mls_next').show();

            form.find('.cf7mls_current_fs').css({
                height: 'auto',
                overflow: 'visible',
                opacity: '1',
                'visibility': 'visible'
            });

            form.find('input').removeAttr('disabled');
            form.find('button').removeAttr('disabled');
            form.find('select').removeAttr('disabled');
            form.find('textarea').removeAttr('disabled');

            form.removeClass('cf7mls_reviewing');
            $('body').find('.cf7mls_reviewing_overlay').remove();
        }        

        preview_clicked = !preview_clicked;

    });
    function cf7mls_is_not_last_fieldset(fieldset, total_fieldset) {
        var order = fieldset.data('cf7mls-order');
        return order != total_fieldset;
    }
    function dhScrollTo(el) {
        if (cf7mls_object.scroll_step == "true") {
            $('html, body').animate({
                scrollTop: el.offset().top
            }, 'slow');
        } else if (cf7mls_object.scroll_step == "scroll_to_top") {
            $('html, body').animate({
                scrollTop: $('body').offset().top
            }, 'slow');
        }
    }
    function cf7mls_toggle_next_btn(acceptances, fieldset) {
        if (acceptances.length > 0) {
            var ii = 0;
            jQuery.each(acceptances, function(i, v) {
                if (jQuery(v).is(':checked')) {
                    //console.log('checked');
                } else {
                    ii++;
                }
            });
            if (ii > 0) {
                //console.log(ii);
                jQuery(fieldset).find('.cf7mls_next').attr('disabled', 'disabled');
            } else {
                jQuery(fieldset).find('.cf7mls_next').removeAttr('disabled');
            }
        }
    }
})(jQuery)