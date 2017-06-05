package com.esgyn.dataloader;

import com.esgyn.source.ISource;
import com.esgyn.target.ITarget;

public interface IDataLoader {
	
	
	public void loadSourToTar(ISource source,ITarget target);

}
