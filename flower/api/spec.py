from __future__ import absolute_import

import logging
import json

from apispec import APISpec, Path
from apispec.exceptions import APISpecError
from pprint import pformat
from tornado import web

from .. import __version__

from ..views import BaseHandler

from ..urls import task_handlers, worker_handlers

logger = logging.getLogger(__name__)

api_spec = APISpec(
    title='Flower',
    version=__version__,
    info=dict(
        description='The flower API spec'
    ),
    plugins=['apispec.ext.tornado']
    #basePath=''
)

api_spec.add_parameter('taskid', 'path', description='The task id', required=True, type='string')
api_spec.add_parameter('taskname', 'path', description='The task name', required=True, type='string')
api_spec.add_parameter('workername', 'path', description='The worker name', required=True, type='string')

for url in task_handlers:
    try:
        api_spec.add_path(urlspec=url)
        logger.info('Registered OpenAPI spec for URI {}'.format(url[0]))
    except APISpecError:
        logger.warning('Error loading OpenAPI spec for URI {}'.format(url[0]))
        continue


for url in worker_handlers:
    try:
        api_spec.add_path(urlspec=url)
        logger.info('Registered OpenAPI spec for URI {}'.format(url[0]))
    except APISpecError:
        logger.warning('Error loading OpenAPI spec for URI {}'.format(url[0]))
        continue

class GetSpec(BaseHandler):
    @web.authenticated
    def get(self):
        """
Get OpenAPI spec

**Example request**:

.. sourcecode:: http

  GET /api/swagger.json HTTP/1.1
  Host: localhost:5555
  User-Agent: HTTPie/0.8.0

**Example response**:

.. sourcecode:: http

  HTTP/1.1 200 OK
  Content-Length: 1109
  Content-Type: application/json; charset=UTF-8
  Etag: "b2478118015c8b825f7b88ce6b660e5449746c37"
  Server: TornadoServer/3.1.1

  paths:
  /api/tasks:
    get:
      responses:
        '200':
          description: Result
      parameters:
        - format: int32
          required: false
          in: query
          name: limit
          description: the maximum number of tasks
          type: integer
        - name: workername
          required: false
          type: string
          in: query
          description: filter task by workername
        - name: taskname
          required: false
          type: string
          in: query
          description: filter task by taskname
        - name: state
          required: false
          type: string
          in: query
          description: filter task by state
      description: List tasks
  /api/task/types:
    get:
      responses:
        '200':
          description: result
      description: List (seen) task types

:reqheader Authorization: optional OAuth token to authenticate
:statuscode 200: no error
:statuscode 401: unauthorized request
        """
        output = api_spec.to_dict()
        self.write(output)
