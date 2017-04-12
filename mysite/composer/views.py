from django.shortcuts import render
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from .forms import SearchForm

def get_name(request):
	if request.method == 'GET':
		form = SearchForm(request.GET)

		if form.is_valid():
			return HttpResponseRedirect('/composerresults/')
	else:
		form = SearchForm()
	
	return render(request, 'composer.html', {'form':form})

# Create your views here.
