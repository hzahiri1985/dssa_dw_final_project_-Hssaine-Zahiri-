from abc import ABCMeta, abstractclassmethod

    
    
    
class AbstractBaseTask(metaclass=ABCMeta):
    """Abstract Base Class of Task that cannot be instantiated and must be
    implemented by the BaseTask class
    """
    @abstractclassmethod
    def validate(self):
        raise NotImplementedError('Abstract Method that needs to be implemented by the subclass')

    @abstractclassmethod
    def run(self):
        raise NotImplementedError('Abstract Method that needs to be implemented by the subclass')
