from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    
    url(r'^home/', include('home.urls')),
    url(r'^search/', include('search.urls')),
    url(r'^foo/', include('foo.urls')),
    url(r'^chord/', include('chord.urls')),
    url(r'^song/', include('song.urls')),
    url(r'^composer/', include('composer.urls')),
    url(r'^chordresults/', include('chordresults.urls')),
    url(r'^songresults/', include('songresults.urls')),
    url(r'^composerresults/', include('composerresults.urls')),
    url(r'^admin/', include(admin.site.urls)),
)
