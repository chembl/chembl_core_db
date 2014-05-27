__author__ = 'mnowotka'

import re
from django.views.generic import TemplateView

#-----------------------------------------------------------------------------------------------------------------------

def plural(string):
    patterns = [('[sxz]$','$','es'),
                ('[^aeioudgkprt]h$','$','es'),
                ('[^aeiou]y$','y$','ies'),
                ('$','$','s')]

    rules = map(lambda (pattern, search, replace): lambda word: re.search(pattern, word) and
                                                                re.sub(search, replace, word), patterns)
    for rule in rules:
        result = rule(string)
        if result:
            return result

#-----------------------------------------------------------------------------------------------------------------------

class DirectTemplateView(TemplateView):
    extra_context = None
    def get_context_data(self, **kwargs):
        context = super(self.__class__, self).get_context_data(**kwargs)
        if self.extra_context is not None:
            for key, value in self.extra_context.items():
                if callable(value):
                    context[key] = value()
                else:
                    context[key] = value
        return context

#-----------------------------------------------------------------------------------------------------------------------
